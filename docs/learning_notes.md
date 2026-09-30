# Retail Lakehouse Learning Notes

## Phase 1 — Understand and prepare the source data

### What are we building?

We are building a local lakehouse pipeline for a multi-store retailer. Data arrives from three different source types: a PostgreSQL operational database, CSV reference files, and a weather API. PySpark processes the data into Bronze, Silver, and Gold Delta tables. Airflow runs the steps in the correct order.

### What did we receive from the trainer?

The trainer supplied the requirements, data dictionary, project plan, sample notebooks, and a dummy-data workbook. The workbook contains the data needed to begin, but it is not the completed pipeline.

### What did we do first?

We exported the supplied workbook and checked every sheet, column, and row count. We then separated the sheets according to the source system they represent.

| Source type | Tables/files | Reason |
| --- | --- | --- |
| PostgreSQL | customers, orders, order_items, payments | These represent transactional operational data. |
| Flat files | products, stores, promotions, suppliers | These are reference/master datasets supplied as files. |
| Weather | weather_sample | This is a reproducible fallback for the Open-Meteo API output. |
| Streaming | stream_events | This supports the optional Structured Streaming exercise. |

### Why do we separate sources?

Real pipelines ingest data from systems with different formats, update patterns, and failure modes. Keeping sources separate makes lineage clear and lets each ingestion job use the correct method. The assessment gives 10% for architecture and source separation.

### Verified row counts

| Dataset | Rows |
| --- | ---: |
| customers | 750 |
| orders | 4,000 |
| order_items | 9,868 |
| payments | 4,000 |
| products | 120 |
| stores | 6 |
| promotions | 15 |
| suppliers | 18 |
| weather_sample | 552 |
| stream_events | 100 |

### Trainer questions and answers

**Is the Excel workbook our final source system?**  
No. It is the supplied seed data. We use its transactional sheets to populate PostgreSQL and its reference sheets to create source CSV files.

**Why validate row counts before transformation?**  
Row counts prove that extraction did not silently lose or duplicate records. They provide a reconciliation point between the supplied source and our pipeline.

**Why keep the original workbook?**  
It is immutable source evidence. We can regenerate all extracted files from it and prove where the data came from.

**What is data lineage?**  
Data lineage records the path of data from its source through Bronze and Silver to the final Gold tables.

**What is the next step?**  
Create PostgreSQL tables with primary and foreign keys, load the four operational CSVs, and verify their database row counts.

## Phase 1B — PostgreSQL source setup

### Why PostgreSQL?

PostgreSQL represents the retailer's operational system, where orders and payments are created. The lakehouse will read from it as one source. PostgreSQL is a relational database, so it provides schemas, primary keys, foreign keys, checks, and transactions.

### Keys used in the operational database

| Table | Primary key | Physical foreign key |
| --- | --- | --- |
| customers | customer_id | — |
| orders | order_id | customer_id → customers.customer_id |
| order_items | order_item_id | order_id → orders.order_id |
| payments | payment_id | order_id → orders.order_id |

`orders.store_id`, `orders.promo_id`, and `order_items.product_id` are logical foreign keys to file-based datasets. PostgreSQL cannot enforce them because those reference tables belong to another source system. PySpark data-quality checks will enforce them after ingestion.

### Why use Docker?

Docker runs PostgreSQL in a reproducible container. Every team member gets the same database version and initialization process without manually installing PostgreSQL. The Compose file defines the service, port, storage volume, health check, and initialization files.

### Why define constraints?

- A primary key prevents missing or duplicate record identifiers.
- A foreign key prevents an operational child record from referencing a missing parent.
- A `NOT NULL` rule protects mandatory fields.
- A `CHECK` rule rejects invalid values such as a negative quantity.
- Indexes speed up joins and incremental reads on commonly searched columns.

### Why is `updated_at` indexed?

The incremental pipeline will later read rows newer than a saved watermark. An index on `updated_at` helps PostgreSQL find those rows without scanning the entire orders table.

### Trainer questions and answers

**What is the difference between a primary key and a foreign key?**  
A primary key uniquely identifies a row in its own table. A foreign key references a key in another table and protects referential integrity.

**Why are store_id and product_id not physical PostgreSQL foreign keys here?**  
Their parent datasets arrive from CSV files rather than PostgreSQL. We enforce those cross-source relationships in the Silver data-quality stage.

**Why use NUMERIC instead of FLOAT for money?**  
`NUMERIC(14,2)` stores exact decimal values. Binary floating-point can introduce small rounding errors, which is unsafe for financial totals.

**What makes the load reproducible?**  
The original workbook is preserved, one script regenerates the CSV files, and PostgreSQL initializes from version-controlled SQL files.

### Commands used

Run these commands from `F:\Big Data\Retail_Lakehouse`:

```powershell
docker compose up -d postgres
docker compose ps postgres
docker compose exec -T postgres psql -U retail_user -d retail -f /tmp/verify_source.sql
```

- `docker compose up -d postgres` creates and starts PostgreSQL in the background.
- `docker compose ps postgres` shows whether the container is running and healthy.
- `psql` is PostgreSQL's command-line client and executes our verification SQL.

### Verified database result

| Table | Required rows | Database rows | Result |
| --- | ---: | ---: | --- |
| customers | 750 | 750 | Passed |
| orders | 4,000 | 4,000 | Passed |
| order_items | 9,868 | 9,868 | Passed |
| payments | 4,000 | 4,000 | Passed |

All three operational relationship checks returned zero failures:

- orders without a customer: 0
- order items without an order: 0
- payments without an order: 0

The saved proof is `evidence/phase_1_postgres_verification.txt`.

### What happened with Docker Desktop?

Docker Desktop initially failed because stale runtime socket files were locked. A socket is a local communication endpoint used by Docker's internal processes. We stopped the stale processes, preserved the temporary runtime folders as backups, and let Docker recreate clean sockets. We did not use factory reset, so Docker data and settings were not erased.

### More trainer questions and answers

**What does a healthy container mean?**  
The container process is running and its configured health check succeeds. Our health check uses `pg_isready` to confirm that PostgreSQL accepts connections.

**Why compare source counts with database counts?**  
Equal counts prove completeness at this ingestion boundary. Counts alone do not prove every value is correct, so we also check keys and later apply more data-quality rules.

**What does zero orphan records mean?**  
Every child foreign key found its required parent. For example, every `order_items.order_id` exists in `orders.order_id`.

**Why does Docker initialize the database only on the first run?**  
The official PostgreSQL image runs files in `/docker-entrypoint-initdb.d` when its data directory is empty. The named volume preserves the initialized database across ordinary container restarts.

**What is the next project step?**  
Implement the plain-Python Open-Meteo weather landing job, including a supplied-data fallback, then capture one weather record per store/date as JSON Lines.

## Phase 2 — Weather API landing

### What is an API?

An API is a defined way for one program to request data or actions from another service. Our Python program sends an HTTP GET request to Open-Meteo with a store's latitude, longitude, date range, and required daily weather fields.

### Why do stores need coordinates?

Weather belongs to a geographic location. Each store has latitude and longitude, so the same function can request the correct weather for Lahore, Islamabad, Karachi, Rawalpindi, Faisalabad, and Multan.

### What does the program request?

- mean temperature at 2 metres
- total precipitation
- mean relative humidity at 2 metres
- maximum wind speed at 10 metres
- daily weather code

The date range is 2026-06-01 through 2026-08-31. That gives 92 days for each of 6 stores, or 552 expected records.

### Why use JSON Lines?

JSON Lines stores one JSON object on each line. It is convenient for append-oriented landing zones, distributed readers, failure recovery, and line-by-line inspection. A normal JSON array wraps the whole file in one large structure, while JSON Lines keeps records independent.

### What is the landing layer?

The landing layer stores data in the shape received or generated by the source extraction program, before Spark turns it into a Bronze Delta table. It provides a replayable boundary between the external API and the lakehouse.

### Why have a fallback?

External APIs can fail because of internet problems, rate limits, maintenance, or timeouts. In `auto` mode, our program tries Open-Meteo first. If the request fails, it uses the trainer-supplied weather sample. This lets the training pipeline remain reproducible and testable offline.

### Why add `_extraction_ts`?

The extraction timestamp records when our pipeline collected the record. This is ingestion metadata, not the date when the weather happened. It supports lineage, debugging, and rerun analysis.

### Why write a temporary file and replace the final file?

If the program crashes halfway through writing, readers should not see a partial landing file. The program completes a temporary file first and then replaces the final file in one operation.

### Commands

```powershell
python ingestion/weather_api.py --mode auto
python ingestion/weather_api.py --mode fallback
```

- `auto`: try the API, then use fallback data on failure.
- `api`: require a successful API call and fail otherwise.
- `fallback`: deliberately use the supplied sample for an offline demonstration.

### Trainer questions and answers

**Why is this plain Python rather than Spark?**  
Calling a small external API for six stores is a lightweight extraction task. Spark is designed for distributed data processing and would add unnecessary overhead here.

**Does Airflow call the API itself?**  
No. Airflow will schedule and monitor this Python program. The program performs the extraction; Airflow performs orchestration.

**How many weather records should we land?**  
Six stores multiplied by 92 dates equals 552 records, with one record per `store_id + observed_date`.

**How do we avoid partially written files?**  
We write to a temporary file, validate the records, and atomically replace the final landing file only after success.

**What is the next step?**  
Build Bronze Delta ingestion for PostgreSQL, flat files, and the weather landing file using PySpark.

### Verified Phase 2 result

Both execution paths passed:

- fallback mode landed 552 records from the supplied sample;
- auto mode successfully landed 552 records from Open-Meteo;
- all six store IDs were present;
- the date range was 2026-06-01 through 2026-08-31;
- the unique business grain was `store_id + observed_date`.

The final landing file currently contains the successful Open-Meteo result. The evidence summary is saved at `evidence/phase_2_weather_verification.txt`.

## Phase 3 — Bronze Delta ingestion

### What is Bronze?

Bronze is the first durable lakehouse layer. It preserves source data with minimal changes and adds technical metadata so every record can be traced back to its origin and ingestion time.

### What enters Bronze?

- PostgreSQL: customers, orders, order_items, payments
- Flat files: products, stores, promotions, suppliers
- Weather landing file: weather

This produces nine Bronze Delta tables.

### What changes in Bronze?

We add three technical columns:

- `_ingestion_ts`: when Spark ingested the record;
- `_source_system`: PostgreSQL, flat file, or weather landing;
- `_source_object`: the source table or file path.

Business fields remain unchanged. Cleaning, deduplication, type enforcement, and cross-source key checks belong in Silver.

### Why use Spark JDBC for PostgreSQL?

JDBC is a standard database connectivity interface. Spark's JDBC reader returns database rows as a DataFrame, allowing the same Spark APIs to process database and file sources. The PostgreSQL JDBC driver translates between Spark and PostgreSQL.

### Why use Delta instead of ordinary Parquet?

Delta stores data in Parquet files plus a transaction log. The log supports reliable commits, schema tracking, table history, and later operations such as `MERGE` for incremental loading.

### What does idempotent mean here?

An idempotent operation produces the same logical result when repeated with the same input. The initial full-snapshot Bronze job uses overwrite mode per table. Running it twice produces the same row counts instead of duplicating every record.

### Why are CSV columns strings in Bronze?

CSV does not carry reliable data types. Keeping file values as strings in Bronze preserves the original representation. Silver will explicitly cast fields according to the data dictionary and handle invalid values visibly.

### Why does PostgreSQL already appear typed?

PostgreSQL has a declared schema. JDBC maps its database types into Spark types, so dates, timestamps, booleans, and decimals already have meaningful types when Spark reads them.

### Commands

```powershell
docker compose build spark
docker compose run --rm spark
```

The first command creates a consistent Spark environment with Java 17, PySpark, and Delta Lake. The second runs the Bronze job in a temporary Spark container connected to PostgreSQL.

### Trainer questions and answers

**What belongs in Bronze versus Silver?**  
Bronze preserves source records and adds ingestion metadata. Silver applies business-ready schemas, cleaning, deduplication, derived columns, and key-integrity rules.

**Why keep separate Bronze tables?**  
Separate tables preserve source lineage and allow one source to be reprocessed without rebuilding unrelated sources.

**Why is overwrite safe for this first implementation?**  
This stage represents a reproducible full snapshot. Overwrite prevents duplicates on reruns. The required incremental improvement will later use watermarks and Delta `MERGE` for changed records.

**Does Spark replace PostgreSQL?**  
No. PostgreSQL is an operational source. Spark reads and processes its data; Delta Lake stores the analytical lakehouse layers.

**What should we verify?**  
Every Bronze path must be readable as Delta, match its source row count, and include all three metadata columns.

### Verified Phase 3 result

The Bronze job ran twice using the same inputs. The second run produced the same counts, proving full-snapshot reruns do not duplicate records.

| Bronze table | Rows | Metadata nulls | Result |
| --- | ---: | ---: | --- |
| customers | 750 | 0 | Passed |
| orders | 4,000 | 0 | Passed |
| order_items | 9,868 | 0 | Passed |
| payments | 4,000 | 0 | Passed |
| products | 120 | 0 | Passed |
| stores | 6 | 0 | Passed |
| promotions | 15 | 0 | Passed |
| suppliers | 18 | 0 | Passed |
| weather | 552 | 0 | Passed |

Every table was reopened using `spark.read.format("delta")`, so the test verifies actual Delta output rather than only checking that directories exist. Evidence is saved at `evidence/phase_3_bronze_verification.txt` and `evidence/phase_3_bronze_manifest.json`.

### Running Spark files from VS Code

Do not use `Run Cell` or the Interactive Window for the Bronze Python files. That feature runs the selected code through VS Code's currently selected local Python interpreter and requires `ipykernel`. Our PySpark and Delta dependencies intentionally live inside the Docker Spark image, so a local interpreter may show unresolved imports or an `ipykernel` error even though the pipeline works.

Use one of these methods from the project root:

```powershell
docker compose run --rm spark
docker compose run --rm spark python spark_jobs/bronze/verify_bronze.py
```

Or use VS Code:

1. Open the entire `F:\Big Data\Retail_Lakehouse` folder.
2. Select **Terminal → Run Task**.
3. Choose **Retail: Run Bronze** or **Retail: Verify Bronze**.

**Why must the whole folder be open?**  
VS Code uses `${workspaceFolder}` as the working directory. If only a Python file is opened, it cannot reliably find `docker-compose.yml`, `.env`, data folders, or relative paths.

**Why not install `ipykernel` into the MSYS Python shown in the screenshot?**  
`ipykernel` would only make the Interactive Window start. It would not reproduce our container's Java, Spark, Delta, JDBC driver, Docker network, or environment variables. The Docker task runs the same verified environment used by the pipeline.
