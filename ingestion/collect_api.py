"""Collecte des flux GBFS TBM vers la couche bronze (JSON brut horodaté).

Le JSON est figé tel que reçu de l'API : aucune transformation avant écriture.
Chemins partitionnés : data/landing/<feed>/date=AAAA-MM-JJ/<horodatage>.json.
Un re-run avec le même horodatage écrase le même fichier (idempotence).
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

GBFS_ROOT_URL = "https://bdx.mecatran.com/utw/ws/gbfs/bordeaux/v3/gbfs.json"
# Clé publique du flux open data Bordeaux Métropole : pas un secret, sert de
# repli quand GBFS_API_KEY n'est pas défini dans l'environnement.
DEFAULT_API_KEY = "opendata-bordeaux-metropole-flux-gtfs-rt"
DEFAULT_LANDING_DIR = Path("data/landing")
REQUEST_TIMEOUT = 30

FEEDS = (
    "station_information",
    "station_status",
    "vehicle_types",
    "system_information",
)


def _api_key() -> str:
    """Retourne la clé API GBFS : variable d'environnement sinon clé publique.

    Le flux Bordeaux Métropole est en open data avec une clé publique fixe
    (`DEFAULT_API_KEY`). `GBFS_API_KEY` permet de la surcharger sans toucher au code.
    """
    return os.environ.get("GBFS_API_KEY") or DEFAULT_API_KEY


def fetch_feed_urls(session: requests.Session | None = None) -> dict[str, str]:
    """Interroge le point d'entrée GBFS et retourne {nom_de_flux: url}.

    Gère le format v3 (data.feeds) avec repli sur le format v2
    (data.<langue>.feeds).
    """
    http = session or requests.Session()
    response = http.get(
        GBFS_ROOT_URL, params={"apiKey": _api_key()}, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    data = response.json()["data"]

    feeds = data.get("feeds")
    if feeds is None:  # format v2 : un bloc par langue
        first_language = next(iter(data.values()))
        feeds = first_language["feeds"]
    return {feed["name"]: feed["url"] for feed in feeds}


def fetch_feed(feed_name: str, session: requests.Session | None = None) -> dict:
    """Récupère le JSON brut d'un sous-flux GBFS (ex. station_status)."""
    http = session or requests.Session()
    urls = fetch_feed_urls(session=http)
    if feed_name not in urls:
        raise KeyError(f"Flux GBFS inconnu : {feed_name} (disponibles : {sorted(urls)})")
    # Les URLs de sous-flux renvoyées par le point d'entrée portent déjà l'apiKey.
    response = http.get(urls[feed_name], timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def save_raw_json(
    payload: dict,
    feed_name: str,
    collected_at: datetime,
    base_dir: Path = DEFAULT_LANDING_DIR,
) -> Path:
    """Écrit le JSON brut horodaté en partition date=AAAA-MM-JJ et retourne le chemin.

    `collected_at` doit être en UTC : c'est notre horodatage d'observation,
    distinct du `last_reported` fourni par TBM.
    """
    if collected_at.utcoffset() != timezone.utc.utcoffset(None):
        raise ValueError("collected_at doit être un datetime UTC explicite.")

    partition = collected_at.strftime("date=%Y-%m-%d")
    stamp = collected_at.strftime("%Y%m%dT%H%M%SZ")
    target = base_dir / feed_name / partition / f"{stamp}.json"
    target.parent.mkdir(parents=True, exist_ok=True)

    enriched = {"collected_at": collected_at.isoformat(), "payload": payload}
    target.write_text(json.dumps(enriched, ensure_ascii=False), encoding="utf-8")
    return target


def collect(feed_name: str, base_dir: Path = DEFAULT_LANDING_DIR) -> Path:
    """Pipeline unitaire : appel GBFS puis écriture du JSON brut. Retourne le chemin."""
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)
    payload = fetch_feed(feed_name)
    return save_raw_json(payload, feed_name, collected_at, base_dir)


def main() -> None:
    """Point d'entrée CLI : python -m ingestion.collect_api station_status."""
    parser = argparse.ArgumentParser(description="Collecte d'un flux GBFS TBM.")
    parser.add_argument("feed", choices=FEEDS, help="Sous-flux GBFS à collecter")
    parser.add_argument(
        "--base-dir", type=Path, default=DEFAULT_LANDING_DIR, help="Répertoire landing"
    )
    args = parser.parse_args()
    path = collect(args.feed, args.base_dir)
    print(f"JSON brut écrit : {path}")


if __name__ == "__main__":
    main()
