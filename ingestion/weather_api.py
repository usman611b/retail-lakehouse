"""Land daily store weather from Open-Meteo with a supplied-data fallback."""

from __future__ import annotations

import argparse
import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STORES_FILE = ROOT / "data" / "source" / "flat_files" / "stores.csv"
FALLBACK_FILE = ROOT / "data" / "source" / "weather" / "weather_sample.csv"
LANDING_FILE = ROOT / "data" / "landing" / "weather" / "weather.jsonl"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_FIELDS = [
    "temperature_2m_mean",
    "precipitation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
    "weather_code",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def request_store_weather(
    store: dict[str, str], start_date: str, end_date: str
) -> list[dict[str, Any]]:
    params = {
        "latitude": store["latitude"],
        "longitude": store["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "Asia/Karachi",
    }
    url = f"{API_URL}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "netsol-retail-lakehouse-training/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        payload = json.load(response)

    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    if not dates:
        raise ValueError(f"Open-Meteo returned no daily data for {store['store_id']}")

    records = []
    for index, observed_date in enumerate(dates):
        records.append(
            {
                "store_id": store["store_id"],
                "observed_date": observed_date,
                "temperature_c": daily["temperature_2m_mean"][index],
                "rainfall_mm": daily["precipitation_sum"][index],
                "humidity_pct": daily["relative_humidity_2m_mean"][index],
                "wind_speed_kmh": daily["wind_speed_10m_max"][index],
                "weather_code": daily["weather_code"][index],
                "source": "open_meteo",
            }
        )
    return records


def fetch_api_records(start_date: str, end_date: str) -> list[dict[str, Any]]:
    stores = read_csv(STORES_FILE)
    records: list[dict[str, Any]] = []
    for store in stores:
        records.extend(request_store_weather(store, start_date, end_date))
    return records


def fallback_records(start_date: str, end_date: str) -> list[dict[str, Any]]:
    records = read_csv(FALLBACK_FILE)
    selected = [
        record
        for record in records
        if start_date <= record["observed_date"] <= end_date
    ]
    for record in selected:
        record["source"] = "dummy_fallback"
        for field in (
            "temperature_c",
            "rainfall_mm",
            "humidity_pct",
            "wind_speed_kmh",
            "weather_code",
        ):
            value = record[field]
            record[field] = float(value) if value != "" else None
        if record["humidity_pct"] is not None:
            record["humidity_pct"] = int(record["humidity_pct"])
        if record["weather_code"] is not None:
            record["weather_code"] = int(record["weather_code"])
    return selected


def validate(records: list[dict[str, Any]]) -> None:
    if not records:
        raise ValueError("No weather records were produced")
    keys = [(record["store_id"], record["observed_date"]) for record in records]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate store/date weather records were produced")
    required = {
        "store_id",
        "observed_date",
        "temperature_c",
        "rainfall_mm",
        "humidity_pct",
        "wind_speed_kmh",
        "weather_code",
        "source",
    }
    for record in records:
        missing = required - record.keys()
        if missing:
            raise ValueError(f"Weather record is missing fields: {sorted(missing)}")


def write_json_lines(records: list[dict[str, Any]]) -> None:
    LANDING_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = LANDING_FILE.with_suffix(".jsonl.tmp")
    extraction_time = datetime.now(timezone.utc).isoformat()
    with temporary_file.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            output = {**record, "_extraction_ts": extraction_time}
            handle.write(json.dumps(output, ensure_ascii=False) + "\n")
    os.replace(temporary_file, LANDING_FILE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", default="2026-06-01")
    parser.add_argument("--end-date", default="2026-08-31")
    parser.add_argument(
        "--mode",
        choices=("auto", "api", "fallback"),
        default="auto",
        help="auto tries Open-Meteo and uses supplied data if the request fails",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.start_date > args.end_date:
        raise ValueError("start-date must be before or equal to end-date")

    if args.mode == "fallback":
        records = fallback_records(args.start_date, args.end_date)
    else:
        try:
            records = fetch_api_records(args.start_date, args.end_date)
        except Exception as error:
            if args.mode == "api":
                raise
            print(f"Open-Meteo request failed; using supplied fallback: {error}")
            records = fallback_records(args.start_date, args.end_date)

    validate(records)
    write_json_lines(records)
    sources = sorted({record["source"] for record in records})
    print(f"Landed {len(records)} weather records at {LANDING_FILE}")
    print(f"Source: {', '.join(sources)}")


if __name__ == "__main__":
    main()

