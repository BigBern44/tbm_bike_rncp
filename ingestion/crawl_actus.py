"""Web crawling des actualités du site public TBM (infotbm.com).

Parcourt la page de listing, suit un nombre borné de liens d'articles et fige
le contenu en JSON brut horodaté dans la couche landing.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ACTUS_URL = "https://www.infotbm.com/fr/actualites"
DEFAULT_LANDING_DIR = Path("data/landing")
REQUEST_TIMEOUT = 30
USER_AGENT = "observatoire-velos-tbm/0.1 (usage pedagogique)"
MAX_ARTICLES = 10
DELAI_ENTRE_REQUETES_S = 1.0  # politesse vis-à-vis du site public


def fetch_html(url: str, session: requests.Session | None = None) -> str:
    """Télécharge une page et retourne le HTML brut."""
    http = session or requests.Session()
    response = http.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    return response.text


def extract_article_links(html: str, base_url: str = ACTUS_URL) -> list[str]:
    """Extrait les liens d'articles du listing, dédoublonnés, même domaine."""
    soup = BeautifulSoup(html, "html.parser")
    domaine = urlparse(base_url).netloc

    liens: list[str] = []
    for ancre in soup.find_all("a", href=True):
        url = urljoin(base_url, ancre["href"])
        if urlparse(url).netloc != domaine:
            continue
        if "/actualites/" not in urlparse(url).path:
            continue
        if url not in liens:
            liens.append(url)
    return liens


def parse_article(html: str, url: str) -> dict[str, str]:
    """Extrait titre et texte d'une page d'article."""
    soup = BeautifulSoup(html, "html.parser")
    titre = soup.find("h1")
    corps = soup.find("article") or soup.find("main") or soup.body
    return {
        "url": url,
        "titre": titre.get_text(strip=True) if titre else "",
        "texte": corps.get_text(separator=" ", strip=True) if corps else "",
    }


def crawl_actus(max_articles: int = MAX_ARTICLES) -> list[dict[str, str]]:
    """Crawle le listing puis chaque article (borné), avec délai de politesse."""
    session = requests.Session()
    liens = extract_article_links(fetch_html(ACTUS_URL, session))

    articles: list[dict[str, str]] = []
    for url in liens[:max_articles]:
        articles.append(parse_article(fetch_html(url, session), url))
        time.sleep(DELAI_ENTRE_REQUETES_S)
    return articles


def save_actus(
    articles: list[dict[str, str]],
    collected_at: datetime,
    base_dir: Path = DEFAULT_LANDING_DIR,
) -> Path:
    """Écrit les articles en JSON brut horodaté et retourne le chemin."""
    partition = collected_at.strftime("date=%Y-%m-%d")
    stamp = collected_at.strftime("%Y%m%dT%H%M%SZ")
    target = base_dir / "actus" / partition / f"{stamp}.json"
    target.parent.mkdir(parents=True, exist_ok=True)

    document = {
        "collected_at": collected_at.isoformat(),
        "source": ACTUS_URL,
        "articles": articles,
    }
    target.write_text(json.dumps(document, ensure_ascii=False), encoding="utf-8")
    return target


def main() -> None:
    """Point d'entrée CLI : python -m ingestion.crawl_actus."""
    collected_at = datetime.now(timezone.utc).replace(microsecond=0)
    articles = crawl_actus()
    path = save_actus(articles, collected_at)
    print(f"{len(articles)} article(s) écrit(s) : {path}")


if __name__ == "__main__":
    main()
