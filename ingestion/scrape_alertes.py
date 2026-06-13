"""Scraping des alertes publiées sur le site public TBM (infotbm.com).

Sortie : JSON brut horodaté dans la couche landing, même convention de
partitionnement que les flux GBFS (date=AAAA-MM-JJ/<horodatage>.json).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

ALERTES_URL = "https://www.infotbm.com/fr/alertes"
DEFAULT_LANDING_DIR = Path("data/landing")
REQUEST_TIMEOUT = 30
USER_AGENT = "observatoire-velos-tbm/0.1 (usage pedagogique)"


def fetch_page(url: str = ALERTES_URL) -> str:
    """Télécharge la page d'alertes TBM et retourne le HTML brut."""
    response = requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.text


def parse_alertes(html: str) -> list[dict[str, str]]:
    """Extrait les alertes du HTML : titre, lien et texte de chaque bloc.

    Le parsing reste volontairement défensif : les blocs candidats sont les
    balises <article> puis, à défaut, les éléments dont la classe contient
    « alert ». Une page sans alerte retourne une liste vide.
    """
    soup = BeautifulSoup(html, "html.parser")
    blocs = soup.find_all("article") or soup.select("[class*=alert]")

    alertes: list[dict[str, str]] = []
    for bloc in blocs:
        titre = bloc.find(["h2", "h3"])
        lien = bloc.find("a", href=True)
        texte = bloc.get_text(separator=" ", strip=True)
        if not texte:
            continue
        alertes.append(
            {
                "titre": titre.get_text(strip=True) if titre else "",
                "lien": lien["href"] if lien else "",
                "texte": texte,
            }
        )
    return alertes


def save_alertes(
    alertes: list[dict[str, str]],
    collected_at: datetime,
    base_dir: Path = DEFAULT_LANDING_DIR,
) -> Path:
    """Écrit les alertes en JSON brut horodaté et retourne le chemin."""
    partition = collected_at.strftime("date=%Y-%m-%d")
    stamp = collected_at.strftime("%Y%m%dT%H%M%SZ")
    target = base_dir / "alertes" / partition / f"{stamp}.json"
    target.parent.mkdir(parents=True, exist_ok=True)

    document = {
        "collected_at": collected_at.isoformat(),
        "source": ALERTES_URL,
        "alertes": alertes,
    }
    target.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return target


def main() -> None:
    """Point d'entrée CLI : python -m ingestion.scrape_alertes."""
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)
    alertes = parse_alertes(fetch_page())
    path = save_alertes(alertes, collected_at)
    print(f"{len(alertes)} alerte(s) écrite(s) : {path}")


if __name__ == "__main__":
    main()
