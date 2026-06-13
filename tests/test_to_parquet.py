"""Tests de la conversion JSON GBFS → Parquet typé (Polars)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from ingestion import to_parquet

COLLECTED_AT = datetime(2026, 6, 12, 8, 5, 0, tzinfo=timezone.utc)

PAYLOAD_STATUS_V3 = {
    "data": {
        "stations": [
            {
                "station_id": "B",
                "num_vehicles_available": 5,
                "num_docks_available": 10,
                "is_installed": True,
                "is_renting": True,
                "is_returning": True,
                "last_reported": "2026-06-12T08:04:30Z",
                "vehicle_types_available": [
                    {"vehicle_type_id": "mecanique", "count": 3},
                    {"vehicle_type_id": "electrique", "count": 2},
                ],
            },
            {
                "station_id": "A",
                "num_bikes_available": 0,
                "num_docks_available": 20,
                "is_installed": True,
                "is_renting": False,
                "is_returning": True,
                "last_reported": 1781251200,  # epoch v2, rejouabilité
            },
        ]
    }
}

PAYLOAD_INFORMATION_V3 = {
    "data": {
        "stations": [
            {
                "station_id": "A",
                "name": [{"text": "Pey-Berland", "language": "fr"}],
                "lat": 44.8378,
                "lon": -0.5792,
                "capacity": 20,
                "address": "Place Pey-Berland, Bordeaux",
            }
        ]
    }
}


def test_station_status_schema_et_typage() -> None:
    df = to_parquet.station_status_to_df(PAYLOAD_STATUS_V3, COLLECTED_AT)

    assert df.schema == to_parquet.SCHEMA_STATION_STATUS
    assert df.height == 2
    # Tri déterministe par station_id.
    assert df["station_id"].to_list() == ["A", "B"]
    # num_vehicles_available (v3) mappé sur num_bikes_available.
    assert df["num_bikes_available"].to_list() == [0, 5]
    # Horodatages UTC : RFC 3339 (v3) et epoch (v2) tous deux acceptés.
    assert df["last_reported"][1] == datetime(2026, 6, 12, 8, 4, 30, tzinfo=timezone.utc)
    assert df["collected_at"][0] == COLLECTED_AT


def test_station_status_vehicle_aplatissement() -> None:
    df = to_parquet.station_status_vehicle_to_df(PAYLOAD_STATUS_V3, COLLECTED_AT)

    assert df.schema == to_parquet.SCHEMA_STATION_STATUS_VEHICLE
    assert df.height == 2  # la station A n'a pas de détail par type
    assert df["vehicle_type_id"].to_list() == ["electrique", "mecanique"]
    assert df["count"].sum() == 5


def test_station_information_nom_localise() -> None:
    df = to_parquet.station_information_to_df(PAYLOAD_INFORMATION_V3, COLLECTED_AT)

    assert df.schema == to_parquet.SCHEMA_STATION_INFORMATION
    assert df["name"].to_list() == ["Pey-Berland"]
    assert df["capacity"].dtype == pl.Int32


def test_convert_ecrit_parquet_partitionne_et_idempotent(tmp_path: Path) -> None:
    landing = tmp_path / "landing" / "station_status" / "date=2026-06-12"
    landing.mkdir(parents=True)
    raw = landing / "20260612T080500Z.json"
    raw.write_text(
        json.dumps(
            {"collected_at": COLLECTED_AT.isoformat(), "payload": PAYLOAD_STATUS_V3}
        ),
        encoding="utf-8",
    )
    bronze = tmp_path / "bronze"

    chemins = to_parquet.convert(raw, base_dir=bronze)

    assert chemins == [
        bronze / "station_status" / "date=2026-06-12" / "20260612T080500Z.parquet",
        bronze
        / "station_status_vehicle"
        / "date=2026-06-12"
        / "20260612T080500Z.parquet",
    ]

    # Conversion déterministe : un re-run écrase les mêmes fichiers à
    # contenu identique, jamais de doublon.
    contenu_initial = chemins[0].read_bytes()
    chemins_bis = to_parquet.convert(raw, base_dir=bronze)
    assert chemins_bis == chemins
    assert chemins[0].read_bytes() == contenu_initial
    assert len(list(bronze.rglob("*.parquet"))) == 2

    relu = pl.read_parquet(chemins[0])
    assert relu.schema == to_parquet.SCHEMA_STATION_STATUS
    assert relu.height == 2


def test_convert_flux_non_gere(tmp_path: Path) -> None:
    landing = tmp_path / "landing" / "flux_inconnu" / "date=2026-06-12"
    landing.mkdir(parents=True)
    raw = landing / "20260612T080500Z.json"
    raw.write_text(
        json.dumps({"collected_at": COLLECTED_AT.isoformat(), "payload": {}}),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        to_parquet.convert(raw)
