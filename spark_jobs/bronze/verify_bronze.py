"""Read Bronze Delta tables back and verify counts and metadata."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from build_bronze import BRONZE_ROOT, ROOT, create_spark


EXPECTED_ROWS = {
    "customers": 750,
    "orders": 4000,
    "order_items": 9868,
    "payments": 4000,
    "products": 120,
    "stores": 6,
    "promotions": 15,
    "suppliers": 18,
    "weather": 552,
}
METADATA_COLUMNS = {"_ingestion_ts", "_source_system", "_source_object"}
EVIDENCE_FILE = ROOT / "evidence" / "phase_3_bronze_verification.txt"


def main() -> None:
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    lines = [
        "Retail Lakehouse Phase 3 Bronze Verification",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
    ]

    try:
        for table, expected in EXPECTED_ROWS.items():
            frame = spark.read.format("delta").load(str(BRONZE_ROOT / table))
            actual = frame.count()
            missing_columns = METADATA_COLUMNS - set(frame.columns)
            null_metadata = frame.filter(
                F.col("_ingestion_ts").isNull()
                | F.col("_source_system").isNull()
                | F.col("_source_object").isNull()
            ).count()
            passed = actual == expected and not missing_columns and null_metadata == 0
            lines.append(
                f"{table}: expected={expected}, actual={actual}, "
                f"metadata_nulls={null_metadata}, passed={passed}"
            )
            if not passed:
                raise ValueError(
                    f"Bronze verification failed for {table}: "
                    f"missing metadata={sorted(missing_columns)}"
                )

        lines.append("Overall result: PASSED")
        EVIDENCE_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("\n".join(lines))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
