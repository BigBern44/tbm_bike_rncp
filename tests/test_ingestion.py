"""Tests de la collecte GBFS et du scraping (sans appel réseau réel)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ingestion import collect_api, scrape_alertes

DISCOVERY_V3 = {
    "data": {
        "feeds": [
            {"name": "station_status", "url": "https://example.test/station_status"},
            {
                "name": "station_information",
                "url": "https://example.test/station_information",
            },
        ]
    }
}

STATUS_PAYLOAD = {"data": {"stations": [{"station_id": "1"}]}}


class FakeResponse:
    """Réponse requests minimale pour les tests."""

    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.text = json.dumps(payload)

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class FakeSession:
    """Session qui sert le discovery puis le sous-flux, et trace les URLs."""

    def __init__(self) -> None:
        self.urls: list[str] = []

    def get(self, url: str, params=None, timeout=None) -> FakeResponse:
        self.urls.append(url)
        if url == collect_api.GBFS_ROOT_URL:
            return FakeResponse(DISCOVERY_V3)
        return FakeResponse(STATUS_PAYLOAD)


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GBFS_API_KEY", "cle-de-test")


def test_fetch_feed_urls_v3() -> None:
    urls = collect_api.fetch_feed_urls(session=FakeSession())
    assert urls == {
        "station_status": "https://example.test/station_status",
        "station_information": "https://example.test/station_information",
    }


def test_fetch_feed_inconnu_leve_keyerror() -> None:
    with pytest.raises(KeyError):
        collect_api.fetch_feed("flux_inexistant", session=FakeSession())


def test_fetch_feed_retourne_le_payload() -> None:
    payload = collect_api.fetch_feed("station_status", session=FakeSession())
    assert payload == STATUS_PAYLOAD


def test_cle_api_absente_utilise_cle_publique(monkeypatch: pytest.MonkeyPatch) -> None:
    # Flux open data : sans GBFS_API_KEY, on retombe sur la clé publique par défaut.
    monkeypatch.delenv("GBFS_API_KEY")
    assert collect_api._api_key() == collect_api.DEFAULT_API_KEY
    urls = collect_api.fetch_feed_urls(session=FakeSession())
    assert "station_status" in urls


def test_save_raw_json_partitionne_et_idempotent(tmp_path: Path) -> None:
    collected_at = datetime(2026, 6, 12, 8, 5, 0, tzinfo=timezone.utc)

    chemin = collect_api.save_raw_json(
        STATUS_PAYLOAD, "station_status", collected_at, base_dir=tmp_path
    )

    assert chemin == (
        tmp_path / "station_status" / "date=2026-06-12" / "20260612T080500Z.json"
    )
    document = json.loads(chemin.read_text(encoding="utf-8"))
    assert document["payload"] == STATUS_PAYLOAD
    assert document["collected_at"] == "2026-06-12T08:05:00+00:00"

    # Re-run au même horodatage : même fichier écrasé, jamais de doublon.
    chemin_bis = collect_api.save_raw_json(
        STATUS_PAYLOAD, "station_status", collected_at, base_dir=tmp_path
    )
    assert chemin_bis == chemin
    assert len(list(tmp_path.rglob("*.json"))) == 1


def test_save_raw_json_refuse_horodatage_naif(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        collect_api.save_raw_json(
            STATUS_PAYLOAD, "station_status", datetime(2026, 6, 12), base_dir=tmp_path
        )


def test_parse_alertes_extrait_titre_lien_texte() -> None:
    html = """
    <html><body>
      <article>
        <h2>Station fermée</h2>
        <a href="/fr/alertes/42">Détail</a>
        <p>Maintenance en cours place Pey-Berland.</p>
      </article>
      <article></article>
    </body></html>
    """
    alertes = scrape_alertes.parse_alertes(html)
    assert len(alertes) == 1
    assert alertes[0]["titre"] == "Station fermée"
    assert alertes[0]["lien"] == "/fr/alertes/42"
    assert "Pey-Berland" in alertes[0]["texte"]


def test_parse_alertes_page_vide() -> None:
    assert scrape_alertes.parse_alertes("<html><body></body></html>") == []
