"""Conversion JSON brut GBFS → Parquet typé (Polars), couche bronze.

Typage explicite avant écriture : aucun schéma inféré. Les horodatages sont
en UTC. La conversion est déterministe : un même JSON d'entrée produit
exactement le même fichier Parquet (idempotence par écrasement).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

DEFAULT_BRONZE_DIR = Path("data/bronze")

SCHEMA_STATION_STATUS: dict[str, pl.DataType] = {
    "station_id": pl.Utf8,
    "num_bikes_available": pl.Int32,
    "num_docks_available": pl.Int32,
    "is_installed": pl.Boolean,
    "is_renting": pl.Boolean,
    "is_returning": pl.Boolean,
    "last_reported": pl.Datetime(time_unit="us", time_zone="UTC"),
    "collected_at": pl.Datetime(time_unit="us", time_zone="UTC"),
}

SCHEMA_STATION_INFORMATION: dict[str, pl.DataType] = {
    "station_id": pl.Utf8,
    "name": pl.Utf8,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "capacity": pl.Int32,
    "address": pl.Utf8,
    "collected_at": pl.Datetime(time_unit="us", time_zone="UTC"),
}

SCHEMA_STATION_STATUS_VEHICLE: dict[str, pl.DataType] = {
    "station_id": pl.Utf8,
    "vehicle_type_id": pl.Utf8,
    "count": pl.Int32,
    "collected_at": pl.Datetime(time_unit="us", time_zone="UTC"),
}


def _parse_timestamp(value: object) -> datetime | None:
    """Normalise un horodatage GBFS en datetime UTC.

    GBFS v3 fournit une chaîne RFC 3339 ; v2 un epoch entier. Les deux sont
    acceptés pour la rejouabilité d'anciens JSON bronze.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(timezone.utc)


def _localized_text(value: object) -> str | None:
    """Aplati un champ localisé GBFS v3 ([{text, language}]) en chaîne simple."""
    if isinstance(value, list):
        return value[0]["text"] if value else None
    return value if value is None else str(value)


def load_raw_json(path: Path) -> tuple[dict, datetime]:
    """Charge un JSON landing et retourne (payload GBFS, collected_at UTC)."""
    document = json.loads(path.read_text(encoding="utf-8"))
    collected_at = datetime.fromisoformat(document["collected_at"])
    return document["payload"], collected_at.astimezone(timezone.utc)


def station_status_to_df(payload: dict, collected_at: datetime) -> pl.DataFrame:
    """Construit le DataFrame typé des statuts de stations.

    Le champ v3 `num_vehicles_available` est mappé sur `num_bikes_available`
    (nom retenu par le modèle de données).
    """
    rows = []
    for station in payload["data"]["stations"]:
        bikes = station.get("num_bikes_available", station.get("num_vehicles_available"))
        rows.append(
            {
                "station_id": station.get("station_id"),
                "num_bikes_available": bikes,
                "num_docks_available": station.get("num_docks_available"),
                "is_installed": station.get("is_installed"),
                "is_renting": station.get("is_renting"),
                "is_returning": station.get("is_returning"),
                "last_reported": _parse_timestamp(station.get("last_reported")),
                "collected_at": collected_at,
            }
        )
    return pl.DataFrame(rows, schema=SCHEMA_STATION_STATUS).sort("station_id")


def station_status_vehicle_to_df(payload: dict, collected_at: datetime) -> pl.DataFrame:
    """Construit le DataFrame de liaison station × type de véhicule × comptage."""
    rows = []
    for station in payload["data"]["stations"]:
        for vt in station.get("vehicle_types_available", []):
            rows.append(
                {
                    "station_id": station.get("station_id"),
                    "vehicle_type_id": vt.get("vehicle_type_id"),
                    "count": vt.get("count"),
                    "collected_at": collected_at,
                }
            )
    return pl.DataFrame(rows, schema=SCHEMA_STATION_STATUS_VEHICLE).sort(
        ["station_id", "vehicle_type_id"]
    )


def station_information_to_df(payload: dict, collected_at: datetime) -> pl.DataFrame:
    """Construit le DataFrame typé du référentiel des stations."""
    rows = []
    for station in payload["data"]["stations"]:
        rows.append(
            {
                "station_id": station.get("station_id"),
                "name": _localized_text(station.get("name")),
                "lat": station.get("lat"),
                "lon": station.get("lon"),
                "capacity": station.get("capacity"),
                "address": station.get("address"),
                "collected_at": collected_at,
            }
        )
    return pl.DataFrame(rows, schema=SCHEMA_STATION_INFORMATION).sort("station_id")


def write_parquet(
    df: pl.DataFrame,
    feed_name: str,
    collected_at: datetime,
    base_dir: Path = DEFAULT_BRONZE_DIR,
) -> Path:
    """Écrit le Parquet en partition date=AAAA-MM-JJ et retourne le chemin.

    Un re-run sur le même horodatage écrase le même fichier : jamais de doublon.
    """
    partition = collected_at.strftime("date=%Y-%m-%d")
    stamp = collected_at.strftime("%Y%m%dT%H%M%SZ")
    target = base_dir / feed_name / partition / f"{stamp}.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(target)
    return target


def convert(raw_json_path: Path, base_dir: Path = DEFAULT_BRONZE_DIR) -> list[Path]:
    """Convertit un JSON landing en Parquet bronze. Retourne les chemins écrits.

    Le flux est déduit du chemin (data/landing/<feed>/...). station_status
    produit deux Parquet : les statuts et la liaison vehicle_types.
    """
    feed_name = raw_json_path.parent.parent.name
    payload, collected_at = load_raw_json(raw_json_path)

    if feed_name == "station_status":
        return [
            write_parquet(
                station_status_to_df(payload, collected_at),
                "station_status",
                collected_at,
                base_dir,
            ),
            write_parquet(
                station_status_vehicle_to_df(payload, collected_at),
                "station_status_vehicle",
                collected_at,
                base_dir,
            ),
        ]
    if feed_name == "station_information":
        return [
            write_parquet(
                station_information_to_df(payload, collected_at),
                "station_information",
                collected_at,
                base_dir,
            )
        ]
    raise ValueError(f"Flux non géré pour la conversion Parquet : {feed_name}")


def main() -> None:
    """Point d'entrée CLI : python -m ingestion.to_parquet <chemin_json>."""
    parser = argparse.ArgumentParser(description="Conversion JSON GBFS → Parquet typé.")
    parser.add_argument("json_path", type=Path, help="Chemin du JSON landing")
    parser.add_argument(
        "--base-dir", type=Path, default=DEFAULT_BRONZE_DIR, help="Répertoire bronze"
    )
    args = parser.parse_args()
    for path in convert(args.json_path, args.base_dir):
        print(f"Parquet écrit : {path}")


if __name__ == "__main__":
    main()
