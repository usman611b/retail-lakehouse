# Retail Lakehouse

A local retail data engineering training project using **PostgreSQL 17, PySpark 4.0.0, Delta Lake 4.0.0, and Docker**.

## Implemented scope

- Export and validate ten sheets from a supplied synthetic retail workbook.
- Load four operational tables into PostgreSQL with keys, constraints, and indexes.
- Land daily store weather from Open-Meteo, with an offline sample fallback.
- Read PostgreSQL through JDBC, four reference CSVs, and weather JSON Lines into **nine Bronze Delta tables**.
- Add ingestion timestamp, source-system, and source-object metadata.
- Read Bronze tables back and check row counts and metadata completeness.

Silver/Gold transformations, scheduling, streaming processing, and reporting are future work. The streaming sample is included as source data, but no streaming pipeline is implemented in this version.

## Data and evidence

| Bronze table | Expected rows |
| --- | ---: |
| customers | 750 |
| orders | 4,000 |
| order_items | 9,868 |
| payments | 4,000 |
| products | 120 |
| stores | 6 |
| promotions | 15 |
| suppliers | 18 |
| weather | 552 |
| **Total** | **19,329** |

The [uploaded Bronze verification report](evidence/phase_3_bronze_verification.txt) records matching counts and no null metadata. This is evidence from an earlier local run, not a claim that Docker/Spark was rerun during publication.

## Run locally

Requirements: Docker with Compose, Python 3.11 or later for source preparation, and enough memory for the configured 2 GB Spark driver plus PostgreSQL. The Dockerfile targets an x86-64 environment.

From this directory, on Linux/WSL:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
# Set your local database password in .env.
python scripts/prepare_sources.py
docker compose up -d postgres
docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < database/verify_source.sql
python ingestion/weather_api.py --mode fallback
docker compose build spark
docker compose run --rm spark
docker compose run --rm spark python spark_jobs/bronze/verify_bronze.py
```

On Windows PowerShell, activate with `.\.venv\Scripts\Activate.ps1` and create `.env` with `Copy-Item .env.example .env`. To submit the verification SQL:

```powershell
Get-Content database/verify_source.sql | docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
```

Use `--mode auto` for weather ingestion to try the live API and fall back to the supplied sample if unavailable. The Bronze verifier expects the default sample period, June through August 2026.

## Repository contents

- `database/`: operational schemas, seed loading, and validation SQL.
- `ingestion/`: API/fallback weather landing.
- `spark_jobs/bronze/`: Bronze writes and read-back verification.
- `scripts/`: workbook extraction and row-count validation.
- `data/source/`: small synthetic training CSVs.
- `source_reference/`: supplied dummy-data workbook and data dictionary.
- `evidence/`: saved source and Bronze validation results.
- `docs/learning_notes.md`: development notes, including planned later stages.

Generated Delta/landing data and local credentials are excluded. PostgreSQL initialization scripts run only when the database volume is first created. The Bronze job overwrites snapshots; it does not implement incremental CDC or merge-based ingestion.

## Author

Usman Ali — Big Data Analytics Trainee at NETSOL Technologies Pakistan.

[LinkedIn](https://www.linkedin.com/in/usman-ali-b44358368) · [GitHub](https://github.com/usman611b)
