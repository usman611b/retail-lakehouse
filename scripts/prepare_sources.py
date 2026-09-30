"""Extract and validate the trainer-supplied Retail Lakehouse workbook."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
WORKBOOK = ROOT / "source_reference" / "Retail_Lakehouse_Dummy_Data.xlsx"

SOURCE_GROUPS = {
    "postgres_seed": ["customers", "orders", "order_items", "payments"],
    "flat_files": ["products", "stores", "promotions", "suppliers"],
    "weather": ["weather_sample"],
    "streaming": ["stream_events"],
}

EXPECTED_ROWS = {
    "customers": 750,
    "orders": 4000,
    "order_items": 9868,
    "payments": 4000,
    "products": 120,
    "stores": 6,
    "promotions": 15,
    "suppliers": 18,
    "weather_sample": 552,
    "stream_events": 100,
}


def main() -> None:
    if not WORKBOOK.exists():
        raise FileNotFoundError(f"Supplied workbook not found: {WORKBOOK}")

    actual_sheets = set(pd.ExcelFile(WORKBOOK).sheet_names)
    missing = set(EXPECTED_ROWS) - actual_sheets
    if missing:
        raise ValueError(f"Workbook is missing sheets: {sorted(missing)}")

    manifest: dict[str, dict[str, object]] = {}

    for group, sheets in SOURCE_GROUPS.items():
        output_dir = ROOT / "data" / "source" / group
        output_dir.mkdir(parents=True, exist_ok=True)

        for sheet in sheets:
            frame = pd.read_excel(WORKBOOK, sheet_name=sheet)
            actual_rows = len(frame)
            expected_rows = EXPECTED_ROWS[sheet]
            if actual_rows != expected_rows:
                raise ValueError(
                    f"{sheet}: expected {expected_rows} rows, found {actual_rows}"
                )

            output_file = output_dir / f"{sheet}.csv"
            frame.to_csv(output_file, index=False, date_format="%Y-%m-%d %H:%M:%S")
            manifest[sheet] = {
                "source_group": group,
                "rows": actual_rows,
                "columns": list(frame.columns),
                "file": str(output_file.relative_to(ROOT)),
            }

    manifest_path = ROOT / "evidence" / "phase_1_source_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print("Source preparation passed.")
    for name, details in manifest.items():
        print(f"{name}: {details['rows']} rows -> {details['file']}")


if __name__ == "__main__":
    main()
