"""
Tarrant County Jail Report Archive Builder

New-file-only archive utility. Does not modify the existing daily report pipeline.

Modes:
  initial_14  -> fetches Day 01 through Day 14 from Tarrant County and saves JSON archive files
  daily       -> fetches Day 01 only and saves/updates today's JSON archive file

Output:a
  output/archive/reports/YYYY-MM-DD/report.json
  output/archive/index.json
  output/archive/latest.json
  output/archive/initial_14_day_backfill_manifest.json (initial_14 only)
"""

import argparse
import json
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from typing import Any

import pdfplumber
import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BOOKED_BASE_URL = os.getenv(
    "BOOKED_BASE_URL",
    "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF",
).rstrip("/")

OUT_ROOT = Path("output/archive")
REPORTS_DIR = OUT_ROOT / "reports"
LATEST_PATH = OUT_ROOT / "latest.json"
INDEX_PATH = OUT_ROOT / "index.json"
INITIAL_MANIFEST_PATH = OUT_ROOT / "initial_14_day_backfill_manifest.json"

# ---------------------------------------------------------------------------
# Parsing patterns copied into this standalone archive utility
# ---------------------------------------------------------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")
CITY_STATE_ZIP_RE = re.compile(r"^(?P<city>[A-Z][A-Z \-']+)\s+TX\s+(?P<zip>\d{5})(?:-\d{4})?$")
CITY_STATE_RE = re.compile(r"^(?P<city>[A-Z][A-Z \-']+)\s+TX(?:\s+\d{5}(?:-\d{4})?)?$")
STREET_SUFFIX_RE = re.compile(
    r"\b(AVE|AV|ST|DR|RD|LN|BLVD|CT|CIR|PKWY|HWY|TER|PL|WAY|TRL|LOOP|FWY|SQ|PARK|RUN|HOLW|HOLLOW|ROW|PT|PIKE|CV|COVE)\b"
)
LEADING_STREET_NUM_RE = re.compile(r"^\d{1,6}\s+")
TRAILING_CITY_TX_ZIP_RE = re.compile(r"\s+([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?\s*$")
INLINE_STREET_ADDR_RE = re.compile(
    r"\s+\d{1,6}\s+[A-Z0-9][A-Z0-9 \-']{1,40}\s+(AVE|AV|ST|DR|RD|LN|BLVD|CT|CIR|PKWY|HWY|TER|PL|WAY|TRL|LOOP|FWY|SQ|CV|COVE)\b.*$"
)
EMBEDDED_BOOKING_RE = re.compile(r"(\d{2}-\d{7})")

JUNK_SUBSTRINGS = [
    "INMATES BOOKED IN DURING THE PAST",
    "REPORT DATE:",
    "PAGE:",
    "INMATE NAME IDENTIFIER",
    "CID",
    "BOOK IN DATE",
    "BOOKING NO.",
    "DESCRIPTION",
]

CATEGORY_RULES = [
    ("DWI / Alcohol", ["DWI", "INTOX", "BAC", "DUI", "ALCOHOL", "DRUNK", "INTOXICATED", "PUBLIC INTOX", "OPEN CONT"]),
    ("Drugs / Possession", ["POSS", "POSS CS", "CONTROLLED SUB", "CS", "DRUG", "NARC", "MARIJ", "METH", "COCAINE", "HEROIN", "PARAPH"]),
    ("Family Violence / Assault", ["FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "CHOKE", "STRANG", "DOMESTIC"]),
    ("Theft / Fraud", ["THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "IDENTITY", "STOLEN", "SHOPLIFT"]),
    ("Weapons", ["WEAPON", "FIREARM", "GUN", "UCW", "UNL CARRYING"]),
    ("Evading / Resisting", ["EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"]),
    ("Warrants / Court / Bond", ["WARRANT", "FTA", "FAIL TO APPEAR", "BOND", "PAROLE", "PROBATION"]),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def is_junk_line(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return True
    return any(s in up for s in JUNK_SUBSTRINGS)


def looks_like_address(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return False
    if CITY_STATE_ZIP_RE.match(up) or CITY_STATE_RE.match(up):
        return True
    if LEADING_STREET_NUM_RE.match(up):
        return True
    return STREET_SUFFIX_RE.search(up) is not None


def clean_charge_line(raw: str) -> str:
    if not raw:
        return ""
    s = normalize_ws(raw)
    if is_junk_line(s):
        return ""
    s = INLINE_STREET_ADDR_RE.sub("", s).strip()
    s = TRAILING_CITY_TX_ZIP_RE.sub("", s).strip()
    s = re.sub(r"\s+TX\s+\d{5}(?:-\d{4})?\s*$", "", s).strip()
    return s


def title_city(city: str) -> str:
    city = normalize_ws(city)
    replacements = {
        "Fw": "Fort Worth",
        "Ft Worth": "Fort Worth",
        "Fortworth": "Fort Worth",
    }
    t = city.title()
    return replacements.get(t, t)


def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_ZIP_RE.match(up)
        if m:
            return title_city(m.group("city"))
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_RE.match(up)
        if m:
            return title_city(m.group("city"))
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m2 = re.search(r"([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?$", up)
        if m2:
            return title_city(m2.group(1))
        m3 = re.search(r"\b([A-Z][A-Z \-']+),?\s+TX\s+\d{5}(?:-\d{4})?\b", up)
        if m3:
            return title_city(m3.group(1))
    return "Unknown"


def apply_content_line(rec: dict[str, Any], ln: str) -> None:
    rec.setdefault("addr_lines", [])
    rec.setdefault("charges", [])

    s = normalize_ws(ln)
    if not s or is_junk_line(s):
        return

    bookings = list(BOOKING_RE.finditer(s))
    if bookings:
        pre = s[: bookings[0].start()].strip()
        if pre and looks_like_address(pre):
            rec["addr_lines"].append(pre)
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(s)
            chunk_clean = clean_charge_line(s[start:end].strip(" -\t"))
            if chunk_clean:
                rec["charges"].append(chunk_clean)
        return

    if looks_like_address(s):
        rec["addr_lines"].append(s)
        return

    cleaned = clean_charge_line(s)
    if not cleaned:
        return

    if not rec["charges"]:
        rec["charges"].append(cleaned)
    else:
        rec["charges"][-1] = normalize_ws(rec["charges"][-1] + " " + cleaned)


def finalize_record(rec: dict[str, Any]) -> dict[str, str]:
    cleaned_charges = [clean_charge_line(c) for c in rec.get("charges", []) if c]
    deduped = []
    for c in cleaned_charges:
        if c and c not in deduped:
            deduped.append(c)

    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]

    return {
        "name": rec.get("name", "").strip(),
        "date": rec.get("book_in_date", "").strip(),
        "charges": ", ".join(deduped),
        "city": extract_city_from_addr_lines(addr_lines),
    }

# ---------------------------------------------------------------------------
# Fetch / Parse / Analyze
# ---------------------------------------------------------------------------

def fetch_pdf(day: int) -> tuple[str, bytes]:
    day_str = f"{day:02d}"
    url = f"{BOOKED_BASE_URL}/{day_str}.PDF"
    print(f"Fetching day {day_str}: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return url, r.content


def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict[str, str]]]:
    records: list[dict[str, str]] = []
    pending = None
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        try:
            first_page_text = pdf.pages[0].extract_text() or ""
            m = re.search(r"Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})", first_page_text)
            if not m:
                m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", first_page_text)
            report_dt = datetime.strptime(m.group(1), "%m/%d/%Y") if m else datetime.now()
        except Exception:
            report_dt = datetime.now()

        for page in pdf.pages:
            lines = (page.extract_text(x_tolerance=2, y_tolerance=2) or "").splitlines()
            for ln in [l.strip() for l in lines if l.strip()]:
                if is_junk_line(ln):
                    continue

                mA = NAME_CID_DATE_RE.match(ln)
                if mA:
                    if current:
                        records.append(finalize_record(current))
                    current = {
                        "name": mA.group("name"),
                        "cid": mA.group("cid"),
                        "book_in_date": mA.group("date"),
                        "addr_lines": [],
                        "charges": [],
                    }
                    pending = None
                    continue

                mB = CID_DATE_ONLY_RE.match(ln)
                if mB:
                    if current:
                        records.append(finalize_record(current))
                    current = None
                    pending = (mB.group("cid"), mB.group("date"))
                    continue

                if pending and NAME_ONLY_RE.match(ln):
                    current = {
                        "name": ln,
                        "cid": pending[0],
                        "book_in_date": pending[1],
                        "addr_lines": [],
                        "charges": [],
                    }
                    pending = None
                    continue

                if pending and not current and ln:
                    pending = None

                if current:
                    apply_content_line(current, ln)

        if current:
            records.append(finalize_record(current))

    return report_dt, fix_embedded_booking_numbers(records)


def fix_embedded_booking_numbers(records: list[dict[str, str]]) -> list[dict[str, str]]:
    fixed = []
    for rec in records:
        name = rec.get("name", "")
        match = EMBEDDED_BOOKING_RE.search(name)
        if match:
            booking_start = match.start()
            clean_name = name[:booking_start].strip()
            extra_content = name[booking_start:].strip()
            existing_charges = rec.get("charges", "")
            rec["name"] = clean_name
            rec["charges"] = f"{extra_content}, {existing_charges}" if existing_charges else extra_content
        fixed.append(rec)
    return fixed


def analyze_stats(records: list[dict[str, str]]) -> dict[str, Any]:
    total_bookings = len(records)
    if total_bookings == 0:
        return {"total_bookings": 0, "top_charge": "N/A", "charge_mix": [], "cities": []}

    first_charges = [
        rec.get("charges", "").split(",")[0].strip().upper()
        for rec in records
        if rec.get("charges")
    ]
    charge_counter = Counter(first_charges)
    top_charge = charge_counter.most_common(1)[0][0] if charge_counter else "N/A"

    charge_mix_counts = Counter()
    for rec in records:
        charge_text = (rec.get("charges") or "").upper()
        found_cat = "Other / Unknown"
        for category, keywords in CATEGORY_RULES:
            if any(keyword in charge_text for keyword in keywords):
                found_cat = category
                break
        charge_mix_counts[found_cat] += 1

    charge_mix = []
    for cat, _keywords in CATEGORY_RULES:
        count = charge_mix_counts.get(cat, 0)
        if count > 0:
            charge_mix.append({"label": cat, "pct": round((count / total_bookings) * 100), "count": count})
    other_count = charge_mix_counts.get("Other / Unknown", 0)
    if other_count > 0:
        charge_mix.append({"label": "Other / Unknown", "pct": round((other_count / total_bookings) * 100), "count": other_count})
    charge_mix.sort(key=lambda x: x["count"], reverse=True)

    city_counter = Counter(rec.get("city", "Unknown") for rec in records if rec.get("city") != "Unknown")
    top_cities_raw = city_counter.most_common(9)
    cities = [{"city": city, "pct": round((count / total_bookings) * 100), "count": count} for city, count in top_cities_raw]
    known_city_total = sum(c["count"] for c in cities)
    other_city_count = total_bookings - known_city_total
    if other_city_count > 0:
        cities.append({"city": "All Other Cities", "pct": round((other_city_count / total_bookings) * 100), "count": other_city_count})

    return {
        "total_bookings": total_bookings,
        "top_charge": top_charge,
        "charge_mix": charge_mix,
        "cities": cities,
    }

# ---------------------------------------------------------------------------
# Archive writing
# ---------------------------------------------------------------------------

def iso_date_from_slash(s: str) -> str:
    return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")


def build_payload(day: int, source_url: str, report_dt: datetime, records: list[dict[str, str]], archive_source: str) -> dict[str, Any]:
    arrests_dt = report_dt - timedelta(days=1)
    report_date = report_dt.strftime("%-m/%-d/%Y")
    arrests_date = arrests_dt.strftime("%-m/%-d/%Y")
    stats = analyze_stats(records)
    bookings = [
        {
            "num": i,
            "name": rec.get("name", ""),
            "date": rec.get("date", arrests_date),
            "charges": rec.get("charges", ""),
            "city": rec.get("city", "Unknown"),
        }
        for i, rec in enumerate(sorted(records, key=lambda x: x.get("name", "")), start=1)
    ]

    return {
        "report_date": report_date,
        "arrests_date": arrests_date,
        "total_bookings": stats["total_bookings"],
        "top_charge": stats["top_charge"],
        "charge_mix": stats["charge_mix"],
        "cities": stats["cities"],
        "bookings": bookings,
        "is_active": day == 1,
        "archive_meta": {
            "archive_source": archive_source,
            "source_day_slot": day,
            "source_url": source_url,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "report_date_iso": report_dt.strftime("%Y-%m-%d"),
            "arrests_date_iso": arrests_dt.strftime("%Y-%m-%d"),
        },
    }


def save_payload(payload: dict[str, Any]) -> Path:
    arrests_iso = payload["archive_meta"]["arrests_date_iso"]
    out_dir = REPORTS_DIR / arrests_iso
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "report.json"
    out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {out_path}")
    return out_path


def rebuild_index() -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    entries = []
    for report_path in sorted(REPORTS_DIR.glob("*/report.json")):
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8"))
            entries.append({
                "arrests_date": payload.get("arrests_date"),
                "report_date": payload.get("report_date"),
                "arrests_date_iso": payload.get("archive_meta", {}).get("arrests_date_iso", report_path.parent.name),
                "report_date_iso": payload.get("archive_meta", {}).get("report_date_iso"),
                "total_bookings": payload.get("total_bookings"),
                "top_charge": payload.get("top_charge"),
                "path": str(report_path),
                "archive_source": payload.get("archive_meta", {}).get("archive_source"),
            })
        except Exception as e:
            print(f"WARNING: Could not index {report_path}: {e}")

    entries.sort(key=lambda x: x.get("arrests_date_iso") or "", reverse=True)
    index_payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(entries),
        "reports": entries,
    }
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(index_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {INDEX_PATH}")

    if entries:
        latest_source_path = Path(entries[0]["path"])
        latest_payload = json.loads(latest_source_path.read_text(encoding="utf-8"))
        LATEST_PATH.write_text(json.dumps(latest_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Saved {LATEST_PATH}")


def run_initial_14() -> None:
    manifest = {
        "archive_name": "initial_14_day_backfill",
        "description": "One-time initial scrape of Tarrant County Daily Booked In Reports day 01 through day 14.",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "reports": [],
    }

    for day in range(1, 15):
        try:
            source_url, pdf_bytes = fetch_pdf(day)
            report_dt, records = parse_booked_in(pdf_bytes)
            payload = build_payload(day, source_url, report_dt, records, "initial_14_day_backfill")
            out_path = save_payload(payload)
            manifest["reports"].append({
                "day": day,
                "source_url": source_url,
                "report_date": payload["report_date"],
                "arrests_date": payload["arrests_date"],
                "total_bookings": payload["total_bookings"],
                "path": str(out_path),
                "status": "saved",
            })
        except Exception as e:
            print(f"ERROR: Failed to archive day {day:02d}: {e}")
            manifest["reports"].append({"day": day, "status": "failed", "error": str(e)})

    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    INITIAL_MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Saved {INITIAL_MANIFEST_PATH}")
    rebuild_index()


def run_daily() -> None:
    source_url, pdf_bytes = fetch_pdf(1)
    report_dt, records = parse_booked_in(pdf_bytes)
    payload = build_payload(1, source_url, report_dt, records, "daily_archive")
    save_payload(payload)
    rebuild_index()


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive Tarrant County daily booked-in reports as JSON.")
    parser.add_argument("--mode", choices=["initial_14", "daily"], required=True)
    args = parser.parse_args()

    if args.mode == "initial_14":
        run_initial_14()
    else:
        run_daily()


if __name__ == "__main__":
    main()
