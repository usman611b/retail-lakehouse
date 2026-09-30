"""Build idempotent Bronze Delta tables from every required source."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = ROOT / "data" / "lakehouse" / "bronze"
EVIDENCE_FILE = ROOT / "evidence" / "phase_3_bronze_manifest.json"

POSTGRES_TABLES = ["customers", "orders", "order_items", "payments"]
FLAT_FILE_TABLES = ["products", "stores", "promotions", "suppliers"]
SPARK_JARS = [
    "/opt/spark-jars/delta-spark.jar",
    "/opt/spark-jars/delta-storage.jar",
    "/opt/spark-jars/antlr4-runtime.jar",
    "/opt/spark-jars/postgresql.jar",
]


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("retail-lakehouse-bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .config("spark.jars", ",".join(SPARK_JARS))
    )
    return builder.getOrCreate()


def add_metadata(
    frame: DataFrame, source_system: str, source_object: str
) -> DataFrame:
    return (
        frame.withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_object", F.lit(source_object))
    )


def read_postgres(spark: SparkSession, table: str) -> DataFrame:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.environ["POSTGRES_DB"]
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option("dbtable", table)
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "1000")
        .load()
    )


def read_flat_file(spark: SparkSession, table: str) -> DataFrame:
    path = ROOT / "data" / "source" / "flat_files" / f"{table}.csv"
    return spark.read.option("header", True).option("inferSchema", False).csv(str(path))


def read_weather(spark: SparkSession) -> DataFrame:
    path = ROOT / "data" / "landing" / "weather" / "weather.jsonl"
    return spark.read.json(str(path))


def write_bronze(frame: DataFrame, table: str) -> int:
    output = BRONZE_ROOT / table
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output))
    )
    return frame.count()


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    manifest: dict[str, dict[str, object]] = {}

    try:
        for table in POSTGRES_TABLES:
            source = read_postgres(spark, table)
            bronze = add_metadata(source, "postgresql", table)
            count = write_bronze(bronze, table)
            manifest[table] = {
                "source_system": "postgresql",
                "rows": count,
                "columns": bronze.columns,
                "path": str((BRONZE_ROOT / table).relative_to(ROOT)),
            }
            print(f"Bronze {table}: {count} rows")

        for table in FLAT_FILE_TABLES:
            source_path = f"data/source/flat_files/{table}.csv"
            source = read_flat_file(spark, table)
            bronze = add_metadata(source, "flat_file", source_path)
            count = write_bronze(bronze, table)
            manifest[table] = {
                "source_system": "flat_file",
                "rows": count,
                "columns": bronze.columns,
                "path": str((BRONZE_ROOT / table).relative_to(ROOT)),
            }
            print(f"Bronze {table}: {count} rows")

        source_path = "data/landing/weather/weather.jsonl"
        source = read_weather(spark)
        bronze = add_metadata(source, "open_meteo_landing", source_path)
        count = write_bronze(bronze, "weather")
        manifest["weather"] = {
            "source_system": "open_meteo_landing",
            "rows": count,
            "columns": bronze.columns,
            "path": str((BRONZE_ROOT / "weather").relative_to(ROOT)),
        }
        print(f"Bronze weather: {count} rows")

        EVIDENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
        evidence = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "write_mode": "overwrite",
            "tables": manifest,
        }
        EVIDENCE_FILE.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        print(f"Evidence: {EVIDENCE_FILE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
