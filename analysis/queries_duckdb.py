"""Requêtes d'analyse sur les marts gold (DuckDB).

Lecture seule du fichier DuckDB matérialisé par dbt. Tout SQL analytique
de transformation reste dans dbt ; ici on ne fait que consommer les marts.
"""

from __future__ import annotations

import os

import duckdb


def get_connection() -> duckdb.DuckDBPyConnection:
    """Ouvre le fichier DuckDB des marts en lecture seule."""
    path = os.environ.get("DUCKDB_PATH", "velos.duckdb")
    return duckdb.connect(path, read_only=True)


def stations_en_tension(
    con: duckdb.DuckDBPyConnection, limite: int = 10
) -> duckdb.DuckDBPyRelation:
    """Stations les plus souvent sans vélo, triées par % de temps en rupture."""
    return con.sql(
        """
        select station_id, nb_observations, pct_temps_sans_velo,
               pct_temps_sans_place, moyenne_velos_disponibles
        from main_gold.mart_tension_stations
        where en_tension
        order by pct_temps_sans_velo desc
        limit ?
        """,
        params=[limite],
    )


def profil_horaire_station(
    con: duckdb.DuckDBPyConnection, station_id: str
) -> duckdb.DuckDBPyRelation:
    """Profil de disponibilité moyenne par heure UTC pour une station."""
    return con.sql(
        """
        select heure_utc, moyenne_velos, moyenne_places, nb_observations
        from main_gold.mart_profil_horaire
        where station_id = ?
        order by heure_utc
        """,
        params=[station_id],
    )


def heures_critiques_reseau(
    con: duckdb.DuckDBPyConnection,
) -> duckdb.DuckDBPyRelation:
    """Heures de la journée où la disponibilité moyenne réseau est la plus basse."""
    return con.sql(
        """
        select heure_utc,
               round(avg(moyenne_velos), 2) as moyenne_velos_reseau,
               sum(nb_observations)         as nb_observations
        from main_gold.mart_profil_horaire
        group by heure_utc
        order by moyenne_velos_reseau asc
        """
    )


def main() -> None:
    """Affiche les principaux indicateurs de l'observatoire."""
    con = get_connection()

    print("=== Stations en tension (top 10) ===")
    stations_en_tension(con).show()

    print("=== Heures critiques du réseau (UTC) ===")
    heures_critiques_reseau(con).show()


if __name__ == "__main__":
    main()
