# CLAUDE.md — Observatoire Vélos TBM (pipeline data)

> Fichier d'instructions pour Claude Code. Décrit le contexte, l'architecture
> lakehouse et les conventions de code. À lire avant toute action.

---

## 1. Contexte

Pipeline data d'un observatoire de la disponibilité des vélos en libre-service du
réseau TBM (Bordeaux Métropole), collectés en temps réel via le flux GBFS. Objectif :
constituer un référentiel historisé fiable pour détecter les stations en tension.

Le projet est volontairement construit en **architecture lakehouse** (stockage objet
+ Parquet + moteur SQL colonne + transformation déclarative) pour démontrer la maîtrise
des standards modernes d'ingénierie data. Ce choix n'est pas imposé par la volumétrie
(~20 M lignes/an) : il est délibéré.

---

## 2. Stack technique imposée

| Brique | Techno | Rôle |
|---|---|---|
| Orchestration | **Dagster** | Software-defined assets, schedules, sensors, intégration dbt |
| Langage | Python 3.11 | Collecte, conversion Parquet |
| Collecte API | `requests` | Appels au flux GBFS |
| Scraping | `requests` + `beautifulsoup4` | Pages publiques TBM (alertes) |
| Conversion Parquet | **Polars** | JSON brut → Parquet typé |
| Stockage objet (data lake) | **MinIO** (S3-compatible) | Couches bronze/silver/gold |
| Transformation déclarative | **dbt** (adapter `dbt-duckdb`) | Staging + marts + tests |
| Moteur SQL / entrepôt | **DuckDB** | Lit le Parquet sur MinIO via httpfs/S3 |

Contraintes : typage explicite avant écriture Parquet ; horodatage UTC ; secrets
(clé API GBFS, credentials MinIO) hors du code (variables d'environnement / ressources
Dagster), jamais commités. Pas de PostgreSQL : DuckDB tient le rôle de base SQL.

---

## 3. Source de données — flux GBFS TBM

Point d'entrée : `https://bdx.mecatran.com/utw/ws/gbfs/bordeaux/v3/gbfs.json?apiKey=<CLE>`
(clé en variable d'environnement `GBFS_API_KEY`).

Sous-flux GBFS v3 :
- `station_information` — référentiel statique (~190 stations), 1×/jour.
- `station_status` — disponibilité temps réel, toutes les 5 min.
- `vehicle_types` — types d'engins (mécanique / électrique).
- `system_information` — métadonnées réseau.

Volumétrie : ~55 000 lignes/jour de statuts (~20 M/an). Aucune donnée personnelle.

---

## 4. Architecture lakehouse (médaillon)

```
                         ┌──────────── Dagster (orchestration, assets) ───────────┐
                         │                                                         │
GBFS API ──requests──► JSON brut (bronze, landing local)                           │
                         │                                                         │
                    Polars: typage + conversion Parquet EN MÉMOIRE                 │
                         ▼   (aucun Parquet sur le disque local)                   │
              Parquet  ──► MinIO  s3://lake/bronze/station_status/date=…/*.parquet │
                         │                                                         │
                    DuckDB (httpfs/S3) lit MinIO                                   │
                         ▼                                                         │
              dbt (dbt-duckdb)                                                     │
                ├─ staging  (silver) : nettoyage, typage, dédoublonnage           │
                └─ marts    (gold)   : agrégats, indicateurs métier  + tests      │
                         ▼                                                         │
              DuckDB marts ──► requêtes d'analyse                                  │
                         └─────────────────────────────────────────────────────────┘
```

Couches médaillon :
- **Bronze** : JSON brut local (landing) + Parquet 1:1 de la source sur MinIO,
  converti en mémoire et téléversé directement — jamais stocké en local
  (rejouabilité, vérité d'origine, pas de saturation disque).
- **Silver** : modèles dbt `staging_*` (nettoyés, typés, dédoublonnés).
- **Gold** : modèles dbt `mart_*` (jointures, agrégats, indicateurs, testés).

Principe directeur : la donnée brute est figée AVANT toute transformation. Toute
transfo dbt est recalculable depuis le bronze sans reperdre le temps réel.

---

## 5. Arborescence du dépôt

```
observatoire-velos-tbm/
├── CLAUDE.md
├── .env.example                  # GBFS_API_KEY, MINIO_* (jamais de vrais secrets)
├── pyproject.toml
│
├── ingestion/
│   ├── collect_api.py             # appels GBFS → JSON brut
│   ├── scrape_alertes.py          # web scraping alertes
│   ├── crawl_actus.py             # web crawling actualités
│   └── to_parquet.py              # JSON → Parquet typé en mémoire (Polars), sans fichier local
│
├── lake/
│   └── minio_client.py            # upload des octets Parquet vers bronze (S3)
│
├── transform/
│   └── dbt_velos/
│       ├── dbt_project.yml
│       ├── profiles.yml           # connexion DuckDB + httpfs/S3 vers MinIO
│       └── models/
│           ├── sources.yml        # déclare le Parquet bronze sur MinIO
│           ├── staging/           # silver
│           │   └── stg_station_status.sql
│           ├── marts/             # gold
│           │   ├── mart_tension_stations.sql
│           │   └── mart_profil_horaire.sql
│           └── schema.yml         # tests not_null / unique / relationships
│
├── orchestration/
│   └── definitions.py             # Dagster : assets, schedules, sensors, dbt
│
├── analysis/
│   └── queries_duckdb.py          # requêtes sur les marts gold
│
└── tests/
    ├── test_ingestion.py
    └── test_to_parquet.py
```

---

## 6. Conventions de code

- Python : PEP 8, type hints, docstrings sur fonctions publiques.
- Secrets via `os.environ` / ressources Dagster. `.env.example` tenu à jour.
- Idempotence : conversion Parquet déterministe (en mémoire, jamais sur disque) ;
  clés objet partitionnées `bronze/station_status/date=AAAA-MM-JJ/{horodatage}.parquet`.
  Réécrire un run écrase le même objet MinIO, jamais de doublon.
- dbt : `stg_` (silver) puis `mart_` (gold) ; toute clé d'unicité et non-nullité
  testée (`unique`, `not_null`) dans `schema.yml`. Pas de SQL analytique hors dbt.
- Distinguer `collected_at` (horodatage d'observation, notre vérité) de
  `last_reported` (horodatage TBM, possiblement plus ancien).
- Dagster : un asset par étape (`raw_*` JSON landing, puis `minio_*` qui convertit
  en mémoire et téléverse le Parquet, puis assets dbt via `dagster-dbt`). Les
  dépendances entre assets reflètent le pipeline.
- Commits : `type(scope): message` (ex. `feat(ingestion): pagination GBFS`).

---

## 7. Modèle de données (schéma en étoile, couche gold)

Table de faits `station_status`, dimensions `station`, `vehicle_type`, liaison
`station_status_vehicle`, métadonnées `system`.

- `station` (clé `station_id`) : name, lat, lon, capacity, address.
- `station_status` (clé `station_id` + `collected_at`) : num_bikes_available,
  num_docks_available, is_renting, is_returning, last_reported.
- `station_status_vehicle` (clé station_id + collected_at + vehicle_type_id) : count.
- `vehicle_type` (clé vehicle_type_id) : form_factor, propulsion_type, max_range.
- `system` (clé system_id) : name, operator, timezone.

Règles d'intégrité portées par les tests dbt (not_null, unique, relationships) et
les contraintes CHECK exprimées dans les modèles (num_*_available >= 0).

---

## 8. Pipeline de bout en bout (assets Dagster)

1. `raw_station_status` — appel GBFS, écrit le JSON brut horodaté (bronze landing local).
2. `minio_station_status` — Polars typé, sérialise le Parquet en mémoire et le
   téléverse vers MinIO `bronze/` (statuts + liaison vehicle_types). Pas de fichier local.
3. assets **dbt** (`dagster-dbt`) — `stg_station_status` (silver) puis
   `mart_tension_stations`, `mart_profil_horaire` (gold), avec tests.

Schedules : collecte `station_status` toutes les 5 min ; `station_information` 1×/jour.
Transformation dbt déclenchée après l'arrivée de nouveaux Parquet (sensor ou schedule).

---

## 9. Périmètre — hors scope de ce dépôt

- Pas de cluster distribué (Spark, Kafka) : DuckDB sur Parquet/MinIO suffit à cette
  volumétrie.
- Pas de PostgreSQL : DuckDB tient le rôle de base SQL ; MinIO/Parquet le stockage objet.
- Pas de déploiement cloud managé : MinIO et l'exécution restent on-premise / local.
