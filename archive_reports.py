import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

from report import (
    BOOKED_BASE_URL,
    fetch_pdf,
    parse_booked_in,
    fix_embedded_booking_numbers,
    analyze_stats,
)

ARCHIVE_DIR = Path("output/archive")
REPORTS_DIR = ARCHIVE_DIR / "reports"


def safe_date_folder(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%m/%d/%Y")
    return dt.strftime("%Y-%m-%d")


def pct_to_number(pct_str: str) -> int:
    if isinstance(pct_str, int):
        return pct_str
    return int(str(pct_str).replace("%", "").strip() or 0)


def build_payload(day_number: int) -> dict:
    booked_day = f"{day_number:02d}"
    pdf_url = f"{BOOKED_BASE_URL.rstrip('/')}/{booked_day}.PDF"

    print(f"Fetching archive day {booked_day}: {pdf_url}")

    pdf_bytes = fetch_pdf(pdf_url)
    report_dt, records = parse_booked_in(pdf_bytes)
    records = fix_embedded_booking_numbers(records)
    stats = analyze_stats(records)

    report_date = report_dt.strftime("%-m/%-d/%Y")
    arrests_date = (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y")
    report_date_display = report_dt.strftime("%A, %B %-d, %Y")

    sorted_records = sorted(records, key=lambda x: x.get("name", ""))

    bookings = []
    for i, rec in enumerate(sorted_records, 1):
        bookings.append({
            "num": i,
            "name": rec.get("name", ""),
            "date": rec.get("book_in_date", arrests_date),
            "charges": rec.get("description", ""),
            "city": rec.get("city", "Unknown"),
        })

    charge_mix = []
    for item in stats.get("charge_mix", []):
        charge_mix.append({
            "label": item[0],
            "pct": pct_to_number(item[1]),
            "count": item[2],
        })

    cities = []
    for item in stats.get("cities", []):
        cities.append({
            "city": item[0],
            "pct": pct_to_number(item[1]),
            "count": item[2],
        })

    return {
        "archive_source_day": booked_day,
        "report_date": report_date,
        "report_date_display": report_date_display,
        "arrests_date": arrests_date,
        "total_bookings": stats.get("total_bookings", 0),
        "top_charge": stats.get("top_charge", "N/A"),
        "charge_mix": charge_mix,
        "cities": cities,
        "bookings": bookings,
        "source": "Tarrant County CJ Reports",
        "archived_at": datetime.utcnow().isoformat() + "Z",
    }


def save_payload(payload: dict):
    folder_name = safe_date_folder(payload["arrests_date"])
    report_dir = REPORTS_DIR / folder_name
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / "report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Saved archive report: {report_path}")


def rebuild_index():
    reports = []

    if REPORTS_DIR.exists():
        for report_path in REPORTS_DIR.glob("*/report.json"):
            try:
                with open(report_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                reports.append({
                    "arrests_date": data.get("arrests_date"),
                    "report_date": data.get("report_date"),
                    "total_bookings": data.get("total_bookings"),
                    "top_charge": data.get("top_charge"),
                    "path": str(report_path),
                })
            except Exception as e:
                print(f"WARNING: Could not read {report_path}: {e}")

    reports.sort(
        key=lambda x: datetime.strptime(x["arrests_date"], "%m/%d/%Y"),
        reverse=True,
    )

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    index_path = ARCHIVE_DIR / "index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(reports, f, indent=2)

    print(f"Saved archive index: {index_path}")

    if reports:
        latest_path = ARCHIVE_DIR / "latest.json"
        latest_report_path = Path(reports[0]["path"])
        with open(latest_report_path, "r", encoding="utf-8") as f:
            latest_data = json.load(f)

        with open(latest_path, "w", encoding="utf-8") as f:
            json.dump(latest_data, f, indent=2)

        print(f"Saved latest archive report: {latest_path}")


def run_backfill():
    print("Starting initial 14-day archive backfill...")

    manifest = {
        "type": "initial_14_day_backfill",
        "started_at": datetime.utcnow().isoformat() + "Z",
        "results": [],
    }

    for day in range(1, 15):
        try:
            payload = build_payload(day)
            save_payload(payload)

            manifest["results"].append({
                "day": f"{day:02d}",
                "success": True,
                "arrests_date": payload.get("arrests_date"),
                "total_bookings": payload.get("total_bookings"),
            })
        except Exception as e:
            print(f"ERROR: Failed archive day {day:02d}: {e}")
            manifest["results"].append({
                "day": f"{day:02d}",
                "success": False,
                "error": str(e),
            })

    manifest["finished_at"] = datetime.utcnow().isoformat() + "Z"

    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = ARCHIVE_DIR / "initial_14_day_backfill_manifest.json"

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    print(f"Saved backfill manifest: {manifest_path}")
    rebuild_index()


def run_daily():
    print("Starting daily archive save for Day 01...")
    payload = build_payload(1)
    save_payload(payload)
    rebuild_index()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        required=True,
        help="backfill = scrape days 01-14, daily = scrape only day 01",
    )

    args = parser.parse_args()

    if args.mode == "backfill":
        run_backfill()
    else:
        run_daily()