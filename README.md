# Observatoire Vélos TBM

Pipeline data d'un observatoire de la disponibilité des **vélos en libre-service du
réseau TBM** (Bordeaux Métropole), collectés en temps réel via le flux **GBFS** et
historisés dans une **architecture lakehouse** (médaillon bronze / silver / gold).

Objectif : constituer un référentiel fiable pour détecter les stations en tension
(régulièrement vides ou pleines) et profiler la disponibilité heure par heure.

> Le lakehouse est un choix **délibéré** (démonstration des standards modernes
> d'ingénierie data), pas une contrainte de volumétrie (~20 M lignes/an).

---

## Stack

| Brique | Techno | Rôle |
|---|---|---|
| Orchestration | **Dagster** | Assets, schedules, sensor, intégration dbt |
| Collecte / conversion | Python 3.11 + `requests` + **Polars** | GBFS → JSON brut → Parquet typé |
| Scraping | `requests` + `beautifulsoup4` | Alertes & actualités du site public TBM |
| Stockage objet (data lake) | **MinIO** (S3-compatible) | Couche bronze (Parquet) |
| Transformation | **dbt** (`dbt-duckdb`) | Staging (silver) + marts (gold) + tests |
| Moteur SQL | **DuckDB** | Lit le Parquet sur MinIO via httpfs/S3 |

Pas de PostgreSQL, pas de Spark/Kafka, pas de cloud managé : DuckDB sur Parquet/MinIO
suffit à cette volumétrie, le tout en local / on-premise.

---

## Architecture (médaillon)

```
GBFS API ──requests──►  JSON brut         (bronze landing : data/landing/, local)
                            │
                       Polars : typage explicite, Parquet sérialisé EN MÉMOIRE
                            │   (aucun Parquet écrit sur le disque local)
                       upload S3
                            ▼
                        MinIO  s3://lake/bronze/<feed>/date=AAAA-MM-JJ/*.parquet
                            │
                       DuckDB (httpfs/S3)
                            ▼
                        dbt ── staging (silver) ──► marts (gold) + tests
                            ▼
                        velos.duckdb  ──►  requêtes d'analyse
```

> **Le Parquet bronze ne vit que sur MinIO.** Il est converti en mémoire puis
> téléversé directement : rien n'est stocké localement sous `data/bronze/`
> (seul le JSON landing reste sur le disque). Cela évite la saturation du disque
> local sur le long terme.

- **Bronze** — JSON brut (local) + Parquet 1:1 sur MinIO. Vérité d'origine figée, rejouable.
- **Silver** — modèles dbt `stg_*` : nettoyés, typés, dédoublonnés (matérialisés en *view*).
- **Gold** — modèles dbt `mart_*` : agrégats et indicateurs métier (matérialisés en *table*), testés.

Principe directeur : la donnée brute est figée **avant** toute transformation. Tout
modèle dbt est recalculable depuis le bronze.

---

## Prérequis

- **[uv](https://docs.astral.sh/uv/)** (gestion de l'environnement Python ; pas de `python3` système requis).
- **MinIO** en binaire standalone (pas de Docker) : `~/.local/bin/minio`, données dans `~/minio-data`.
- Optionnel : la **CLI DuckDB** (`~/.duckdb/cli/`) pour explorer la base en SQL / via l'UI web.

L'environnement Python est créé automatiquement par `dev.sh` au premier démarrage
(`uv venv --python 3.11` puis `uv pip install -e ".[dev]"`).

---

## Configuration

Copier le modèle et le compléter :

```bash
cp .env.example .env
```

| Variable | Rôle | Défaut |
|---|---|---|
| `GBFS_API_KEY` | Clé du flux GBFS — **optionnelle** | clé publique open data Bordeaux Métropole |
| `MINIO_ENDPOINT` | Adresse MinIO | `localhost:9000` |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Credentials MinIO (= root user/password) | — (à remplir) |
| `MINIO_BUCKET` | Bucket du data lake | `lake` |
| `MINIO_USE_SSL` | TLS vers MinIO | `false` |
| `DUCKDB_PATH` | Fichier DuckDB de dbt | `velos.duckdb` |

Les secrets ne sont **jamais** commités : tout passe par l'environnement / `.env`.

---

## Démarrage rapide

Tout est piloté par le script `dev.sh` (idempotent) :

```bash
./dev.sh          # crée le venv au besoin, démarre MinIO + Dagster
./dev.sh status   # état des services
./dev.sh logs     # suit les logs MinIO + Dagster
./dev.sh stop     # arrête tout
```

Une fois démarré :

- **Dagster UI** → http://localhost:3000
- **Console MinIO** → http://localhost:9001

Dans la Dagster UI, tu peux matérialiser les assets à la main ou activer les
schedules / le sensor (onglet *Automation*).

---

## Le pipeline (assets Dagster)

Défini dans [orchestration/definitions.py](orchestration/definitions.py).

| Étape | Asset | Description |
|---|---|---|
| Bronze | `raw_station_status` | Appel GBFS `station_status`, JSON brut horodaté (local, `data/landing/`) |
| Bronze | `minio_station_status` | Conversion Polars typée **en mémoire** (statuts + liaison vehicle_types) puis upload direct vers MinIO `bronze/` |
| Bronze | `raw_/minio_station_information` | Même chaîne pour le référentiel des stations |
| Silver+Gold | `dbt_velos_assets` | `dbt build` : modèles `stg_`/`mart_` + tests |

**Automatisation :**
- `schedule_station_status_5min` — collecte des statuts (cron `*/60 * * * *`).
- `schedule_station_information_quotidien` — référentiel stations (cron `0 4 * * *`).
- `sensor_nouveau_parquet_bronze` — déclenche la transformation dbt dès qu'un nouveau
  Parquet `station_status` arrive sur MinIO.

> ⚠️ Le schedule des statuts est nommé « 5min » mais son cron est **horaire**
> (`*/60`). Le passer à `*/5 * * * *` pour une vraie cadence 5 minutes.

---

## Lancer les étapes à la main (sans Dagster)

Chaque module d'ingestion a un point d'entrée CLI :

```bash
# 1. Collecte GBFS → JSON brut (local, data/landing/)
.venv/bin/python -m ingestion.collect_api station_status
.venv/bin/python -m ingestion.collect_api station_information

# 2. JSON → Parquet typé EN MÉMOIRE, téléversé direct vers MinIO bronze/
#    (aucun Parquet écrit localement ; nécessite les credentials MinIO dans l'env)
.venv/bin/python -m ingestion.to_parquet data/landing/station_status/date=2026-06-13/<horodatage>.json

# Scraping complémentaire (alertes & actualités du site public TBM)
.venv/bin/python -m ingestion.scrape_alertes
.venv/bin/python -m ingestion.crawl_actus
```

> En API : `to_parquet.convert(json_path)` retourne des couples
> `(clé_objet_s3, octets_parquet)` sans rien écrire sur disque, et
> `minio_client.upload_parquet_bytes(clé, octets)` les téléverse.

Transformation dbt directement (depuis `transform/dbt_velos/`, avec le `.env` chargé) :

```bash
cd transform/dbt_velos
../../.venv/bin/dbt build      # modèles staging + marts + tests
../../.venv/bin/dbt test       # tests seuls
```

---

## Modèle de données (gold, schéma en étoile)

- **`station`** (`station_id`) — name, lat, lon, capacity, address.
- **`station_status`** (`station_id` + `collected_at`) — num_bikes_available,
  num_docks_available, is_renting, is_returning, last_reported.
- **`station_status_vehicle`** — comptage par type d'engin.
- **`vehicle_type`** — form_factor, propulsion_type, max_range.
- **`system`** — name, operator, timezone.

Marts produits :
- **`mart_tension_stations`** — % de temps sans vélo / sans place par station ;
  flag `en_tension` (≥ 20 % du temps sans vélo en période de location active).
- **`mart_profil_horaire`** — disponibilité moyenne par heure UTC et par station.

> On distingue toujours `collected_at` (horodatage **d'observation**, notre vérité)
> de `last_reported` (horodatage **TBM**, possiblement plus ancien).

Intégrité garantie par les tests dbt (`not_null`, `unique`, `relationships` dans
[schema.yml](transform/dbt_velos/models/schema.yml)) et des filtres `num_*_available >= 0`.

---

## Analyser les données

### En Python

```bash
DUCKDB_PATH=transform/dbt_velos/velos.duckdb \
  .venv/bin/python analysis/queries_duckdb.py
```

[analysis/queries_duckdb.py](analysis/queries_duckdb.py) ouvre la base en lecture
seule et fournit : stations en tension, profil horaire d'une station, heures
critiques du réseau.

### En SQL (CLI DuckDB)

```bash
cd transform/dbt_velos
duckdb -readonly velos.duckdb
```
```sql
show tables;
select * from main_gold.mart_tension_stations where en_tension
order by pct_temps_sans_velo desc limit 10;
```

### UI web DuckDB

Depuis une session DuckDB ouverte sur la base :

```sql
CALL start_ui();   -- ouvre http://localhost:4213
```

> DuckDB n'autorise **qu'un seul writer** : ferme l'UI / la CLI (ou ouvre en
> `-readonly`) avant de lancer `dbt build`, sinon conflit de verrou sur `velos.duckdb`.

---

## Tests

```bash
.venv/bin/pytest           # tests Python (ingestion, conversion Parquet)
```

Tests data : portés par dbt (`dbt test` ou inclus dans `dbt build`).

---

## Arborescence

```
.
├── ingestion/        # collecte GBFS, scraping, conversion Parquet (Polars)
├── lake/             # client MinIO (upload bronze S3)
├── transform/dbt_velos/   # projet dbt (sources, staging silver, marts gold, tests)
├── orchestration/    # définitions Dagster (assets, schedules, sensor, dbt)
├── analysis/         # requêtes DuckDB sur les marts gold
├── tests/            # tests Python
├── dev.sh            # démarrage / arrêt des services (MinIO + Dagster)
└── CLAUDE.md         # contexte détaillé & conventions du projet
```

Voir [CLAUDE.md](CLAUDE.md) pour le contexte complet et les conventions de code.
