"""
Tarrant County Daily Jail Report (HTML + PDF + Email + Kit Draft + Base44 Sync + JSON)

- Fetches latest booked-in PDF from Tarrant County CJ reports
- Parses booking records
- Calculates stats
- Renders full HTML via daily_report_template.html
- Generates a PDF from HTML using Pyppeteer
- Writes artifacts into /output
- Emails full HTML body + attaches PDF to personal email
- Creates a mobile-friendly Kit draft broadcast with ALL bookings as cards
- Sends structured JSON payload to Base44 for live website updates
"""

import os
import re
import ssl
import smtplib
import asyncio
import html
import json
from io import BytesIO
from datetime import datetime, timedelta
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pdfplumber
import requests
from pyppeteer import launch

# ---------------------------------------------------------------------------
# Config / Env
# ---------------------------------------------------------------------------

BOOKED_BASE_URL = os.getenv(
    "BOOKED_BASE_URL",
    "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
)
BOOKED_DAY = os.getenv("BOOKED_DAY", "01").strip()

TO_EMAIL = os.getenv("TO_EMAIL", "").strip()
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

# Kit API V4 â creates a draft broadcast in Kit
KIT_API_KEY = os.getenv("KIT_API_KEY", "").strip()
KIT_EMAIL_TEMPLATE_ID = os.getenv("KIT_EMAIL_TEMPLATE_ID", "").strip()

# Base44 live report sync
BASE44_AUTOMATION_API_KEY = os.getenv("BASE44_AUTOMATION_API_KEY", "").strip()
BASE44_FUNCTION_URL = os.getenv("BASE44_FUNCTION_URL", "").strip()

# Report CTA used in Kit email
SUBSCRIBER_REPORT_URL = os.getenv(
    "SUBSCRIBER_REPORT_URL",
    "https://dailyjailreports.com/report?access=subscriber"
).strip()

# Defaults if not set as secrets
SMTP_HOST = (os.getenv("SMTP_HOST", "smtp.gmail.com") or "smtp.gmail.com").strip()
if not SMTP_HOST:
    SMTP_HOST = "smtp.gmail.com"

_raw_port = (os.getenv("SMTP_PORT", "465") or "465").strip()
try:
    SMTP_PORT = int(_raw_port)
except ValueError:
    print(f"WARNING: Invalid SMTP_PORT='{_raw_port}'. Falling back to 465.")
    SMTP_PORT = 465

# Template + output paths
HTML_TEMPLATE_PATH = "daily_report_template.html"

OUT_DIR = "output"
HTML_OUTPUT_PATH = os.path.join(OUT_DIR, "daily_jail_report.html")
PDF_OUTPUT_PATH = os.path.join(OUT_DIR, "daily_jail_report.pdf")
JSON_OUTPUT_PATH = os.path.join(OUT_DIR, "daily_jail_report.json")

os.makedirs(OUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Parsing patterns
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

JUNK_SUBSTRINGS = [
    "INMATES BOOKED IN DURING THE PAST", "REPORT DATE:", "PAGE:", "INMATE NAME IDENTIFIER",
    "CID", "BOOK IN DATE", "BOOKING NO.", "DESCRIPTION",
]

CATEGORY_RULES = [
    ("DWI / Alcohol", ["DWI", "DUI", "INTOX", "INTOXICATED", "BAC", "ALCOHOL", "DRUNK", "PUBLIC INTOX", "OPEN CONT", "OPEN CONTAINER"]),
    ("Drugs / Possession", ["POSS", "POSSESSION", "POSS CS", "CONTROLLED SUB", "CONTROLLED SUBSTANCE", "CS", "DRUG", "NARC", "MARIJ", "METH", "COCAINE", "HEROIN", "PARAPH"]),
    ("Family Violence / Assault", ["FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "CHOKE", "STRANG", "STRANGULATION", "DOMESTIC"]),
    ("Theft / Fraud", ["THEFT", "BURGL", "BURGLARY", "ROBB", "ROBBERY", "FRAUD", "FORGERY", "IDENTITY", "STOLEN", "SHOPLIFT"]),
    ("Weapons", ["WEAPON", "FIREARM", "GUN", "UCW", "UNL CARRYING", "UNLAWFUL CARRY"]),
    ("Evading / Resisting", ["EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"]),
    ("Warrants / Court / Bond", ["WARRANT", "FTA", "FAIL TO APPEAR", "BOND", "PAROLE", "PROBATION"]),
]

EMBEDDED_BOOKING_RE = re.compile(r"(\d{2}-\d{7})")

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

def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_ZIP_RE.match(up)
        if m:
            return normalize_ws(m.group("city").title())
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_RE.match(up)
        if m:
            return normalize_ws(m.group("city").title())
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m2 = re.search(r"([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?$", up)
        if m2:
            return normalize_ws(m2.group(1).title())
        m3 = re.search(r"\b([A-Z][A-Z \-']+),?\s+TX\s+\d{5}(?:-\d{4})?\b", up)
        if m3:
            return normalize_ws(m3.group(1).title())
    return "Unknown"

def apply_content_line(rec: dict, ln: str) -> None:
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

def finalize_record(rec: dict) -> dict:
    cleaned_charges = [clean_charge_line(c) for c in rec.get("charges", []) if c]
    deduped = []
    for c in cleaned_charges:
        if c and c not in deduped:
            deduped.append(c)

    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]

    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": extract_city_from_addr_lines(addr_lines),
        "description": ", ".join(deduped),
    }

def pct_str_to_int(pct_str) -> int:
    try:
        return int(str(pct_str).replace("%", "").strip())
    except Exception:
        return 0

def infer_charge_category(charges: str) -> str:
    text = re.sub(r"[^A-Z0-9 <>=/\-]", " ", (charges or "").upper())
    text = normalize_ws(text)
    for category, keywords in CATEGORY_RULES:
        if any(keyword in text for keyword in keywords):
            return category
    return "Other / Unknown"

# ---------------------------------------------------------------------------
# Fetch + Parse
# ---------------------------------------------------------------------------

def fetch_pdf(url: str) -> bytes:
    print(f"Fetching PDF from {url} ...")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    print("PDF fetched.")
    return r.content

def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        try:
            first_page_text = pdf.pages[0].extract_text() or ""
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

    print(f"Parsed {len(records)} booking records.")
    return report_dt, records

def fix_embedded_booking_numbers(records: list[dict]) -> list[dict]:
    print("Fixing embedded booking numbers in names (if any)...")
    fixed = []
    for rec in records:
        name = rec.get("name", "")
        match = EMBEDDED_BOOKING_RE.search(name)
        if match:
            booking_start = match.start()
            clean_name = name[:booking_start].strip()
            extra_content = name[booking_start:].strip()

            existing_desc = rec.get("description", "")
            if existing_desc:
                new_desc = f"{extra_content}, {existing_desc}"
            else:
                new_desc = extra_content

            rec["name"] = clean_name
            rec["description"] = new_desc

        fixed.append(rec)
    return fixed

# ---------------------------------------------------------------------------
# Analyze stats
# ---------------------------------------------------------------------------

def analyze_stats(records: list[dict]) -> dict:
    total_bookings = len(records)
    if total_bookings == 0:
        return {"total_bookings": 0, "top_charge": "N/A", "charge_mix": [], "cities": [], "charge_bars": []}

    first_charges = [
        rec.get("description", "").split(",")[0].strip().upper()
        for rec in records
        if rec.get("description")
    ]
    charge_counter = Counter(first_charges)
    top_charge = charge_counter.most_common(1)[0][0] if charge_counter else "N/A"

    categorized_charges = [(rec.get("description") or "").upper() for rec in records]
    charge_mix_counts = Counter()
    for charge_text in categorized_charges:
        found_cat = infer_charge_category(charge_text)
        charge_mix_counts[found_cat] += 1

    charge_mix = []
    for cat, _keywords in CATEGORY_RULES:
        count = charge_mix_counts.get(cat, 0)
        if count > 0:
            charge_mix.append((cat, f"{round((count / total_bookings) * 100)}%", count))

    other_count = charge_mix_counts.get("Other / Unknown", 0)
    if other_count > 0:
        charge_mix.append(("Other / Unknown", f"{round((other_count / total_bookings) * 100)}%", other_count))

    charge_mix.sort(key=lambda x: x[2], reverse=True)

    cities = [rec.get("city", "Unknown") for rec in records]
    city_counts = Counter(c for c in cities if c != "Unknown")
    top_cities_raw = city_counts.most_common(9)
    top_cities = [(city, f"{round((count / total_bookings) * 100)}%", count) for city, count in top_cities_raw]

    known_city_total = sum(c[2] for c in top_cities)
    unknown_count = total_bookings - known_city_total
    if unknown_count > 0:
        top_cities.append(("All Other Cities", f"{round((unknown_count / total_bookings) * 100)}%", unknown_count))

    charge_bars = []
    for cat, pct_str, count in charge_mix:
        label = {
            "Family Violence / Assault": "Fam. Violence",
            "Drugs / Possession": "Drugs / Poss.",
            "Evading / Resisting": "Evading",
            "Warrants / Court / Bond": "Warrants",
            "Other / Unknown": "Other",
        }.get(cat, cat)

        color = "#a09890" if cat == "Other / Unknown" else "#c8a45a"
        charge_bars.append((label, int(pct_str.replace("%", "")), color))

    return {
        "total_bookings": total_bookings,
        "top_charge": top_charge,
        "charge_mix": charge_mix,
        "cities": top_cities,
        "charge_bars": charge_bars,
    }

# ---------------------------------------------------------------------------
# Payload / JSON
# ---------------------------------------------------------------------------

def build_structured_payload(stats: dict, records: list[dict], report_date_str: str, arrests_date_str: str, report_date_display: str) -> dict:
    sorted_records = sorted(records, key=lambda x: x.get("name", ""))

    bookings = []
    for i, rec in enumerate(sorted_records, 1):
        charges = rec.get("description", "")
        bookings.append({
            "num": i,
            "name": rec.get("name", ""),
            "date": rec.get("book_in_date", arrests_date_str),
            "charges": charges,
            "city": rec.get("city", "Unknown"),
            "charge_category": infer_charge_category(charges),
        })

    return {
        "report_date": report_date_str,
        "report_date_display": report_date_display,
        "arrests_date": arrests_date_str,
        "total_bookings": stats.get("total_bookings", 0),
        "top_charge": stats.get("top_charge", "N/A"),
        "charge_mix": [
            {"label": item[0], "pct": pct_str_to_int(item[1]), "count": item[2]}
            for item in stats.get("charge_mix", [])
        ],
        "cities": [
            {"city": item[0], "pct": pct_str_to_int(item[1]), "count": item[2]}
            for item in stats.get("cities", [])
        ],
        "charge_bars": [
            {"label": item[0], "pct": item[1], "color": item[2]}
            for item in stats.get("charge_bars", [])
        ],
        "top_bookings": bookings[:5],
        "bookings": bookings,
        "is_active": True,
        "source": "Tarrant County CJ Reports",
        "created_from_automation": True,
    }

def save_json_payload(payload: dict):
    with open(JSON_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved JSON to {JSON_OUTPUT_PATH}")

# ---------------------------------------------------------------------------
# Render full report HTML
# ---------------------------------------------------------------------------

def render_html(data: dict) -> str:
    print("Rendering HTML...")

    with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    def build_charge_mix_bars(items):
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            color = "#a09890" if label == "Other / Unknown" else "#c8a45a"
            rows.append(f'''<tr>
              <td style="padding:3px 0; width:140px; color:#666360; font-size:11px; vertical-align:middle;">{html.escape(label)}</td>
              <td style="padding:3px 8px; vertical-align:middle;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
                  <tr><td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td><td style="font-size:1px;">&nbsp;</td></tr>
                </table>
              </td>
              <td style="padding:3px 0; width:70px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%&nbsp;<span style="color:#999590; font-weight:400; font-size:10px;">({count})</span></td>
            </tr>''')
        return "\n".join(rows)

    def build_city_bars(items):
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            color = "#a09890" if label == "All Other Cities" else "#c8a45a"
            label_style = "color:#999590; font-style:italic;" if label == "All Other Cities" else "color:#666360;"
            rows.append(f'''<tr>
              <td style="padding:3px 0; width:140px; {label_style} font-size:11px; vertical-align:middle;">{html.escape(label)}</td>
              <td style="padding:3px 8px; vertical-align:middle;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
                  <tr><td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td><td style="font-size:1px;">&nbsp;</td></tr>
                </table>
              </td>
              <td style="padding:3px 0; width:70px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%&nbsp;<span style="color:#999590; font-weight:400; font-size:10px;">({count})</span></td>
            </tr>''')
        return "\n".join(rows)

    def build_bar_rows(items):
        rows = []
        for label, pct, color in items:
            rows.append(f'''<tr>
              <td style="padding:3px 0; width:140px; color:#666360; font-size:11px; vertical-align:middle;">{html.escape(label)}</td>
              <td style="padding:3px 8px; vertical-align:middle;">
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
                  <tr><td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td><td style="font-size:1px;">&nbsp;</td></tr>
                </table>
              </td>
              <td style="padding:3px 0; width:36px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%</td>
            </tr>''')
        return "\n".join(rows)

    def build_booking_rows(items):
        rows = []
        for i, rec in enumerate(items, 1):
            bg = "#faf8f5" if i % 2 == 1 else "#f4f1eb"
            rows.append(f'''<tr style="background-color:{bg};">
              <td style="padding:9px 12px; color:#999590; font-size:11px; border-bottom:1px solid #e8e4dc; vertical-align:top;">{i}</td>
              <td style="padding:9px 12px; color:#1a1a1a; font-weight:600; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html.escape(rec.get("name", ""))}</td>
              <td style="padding:9px 12px; color:#666360; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html.escape(rec.get("book_in_date", ""))}</td>
              <td style="padding:9px 12px; color:#444240; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:11px;">{html.escape(rec.get("description", ""))}</td>
              <td style="padding:9px 12px; color:#666360; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html.escape(rec.get("city", ""))}</td>
            </tr>''')
        return "\n".join(rows)

    replacements = {
        "{{report_date}}": data.get("report_date", ""),
        "{{report_date_display}}": data.get("report_date_display", ""),
        "{{arrests_date}}": data.get("arrests_date", ""),
        "{{total_bookings}}": str(data.get("total_bookings", 0)),
        "{{top_charge}}": html.escape(data.get("top_charge", "N/A")),
        "{{charge_mix_rows}}": build_charge_mix_bars(data.get("charge_mix", [])),
        "{{city_rows}}": build_city_bars(data.get("cities", [])),
        "{{bar_rows}}": build_bar_rows(data.get("charge_bars", [])),
        "{{booking_rows}}": build_booking_rows(data.get("bookings", [])),
    }

    for placeholder, value in replacements.items():
        template = template.replace(placeholder, value)

    with open(HTML_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(template)

    print(f"Saved HTML to {HTML_OUTPUT_PATH}")
    return template

# ---------------------------------------------------------------------------
# Kit mobile-friendly HTML with ALL bookings as cards
# ---------------------------------------------------------------------------

def build_email_bar_rows(items, label_key="label"):
    rows = []
    for item in items:
        label = item.get(label_key, "")
        pct = int(item.get("pct", 0) or 0)
        count = int(item.get("count", 0) or 0)
        color = "#a09890" if label in ["Other / Unknown", "All Other Cities", "Other"] else "#c8a45a"
        rows.append(f'''
        <tr>
          <td style="padding:5px 0; width:42%; color:#666360; font-size:13px; vertical-align:middle;">{html.escape(label)}</td>
          <td style="padding:5px 10px; vertical-align:middle;">
            <div style="background:#e8e4dc; border-radius:3px; height:12px; width:100%;">
              <div style="background:{color}; width:{pct}%; height:12px; border-radius:3px;"></div>
            </div>
          </td>
          <td style="padding:5px 0; width:70px; color:#1a1a1a; font-size:13px; font-weight:700; text-align:right; vertical-align:middle;">{pct}% <span style="color:#999590; font-weight:400;">({count})</span></td>
        </tr>
        ''')
    return "".join(rows)

def build_kit_booking_cards(bookings: list[dict]) -> str:
    cards = []
    for booking in bookings:
        num = booking.get("num", "")
        name = html.escape(booking.get("name", ""))
        date = html.escape(booking.get("date", ""))
        city = html.escape(booking.get("city", ""))
        charges = html.escape(booking.get("charges", ""))
        cards.append(f'''
        <div style="padding:18px 0; border-bottom:1px solid #e6e0d6;">
          <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:13px; line-height:1.4; letter-spacing:1.5px; text-transform:uppercase;">
            #{num} - {date} - {city}
          </div>
          <div style="font-family:Arial, Helvetica, sans-serif; color:#1a1a1a; font-size:18px; line-height:1.35; font-weight:800; margin-top:6px;">
            {name}
          </div>
          <div style="font-family:Arial, Helvetica, sans-serif; color:#333333; font-size:15px; line-height:1.5; margin-top:8px;">
            {charges}
          </div>
        </div>
        ''')
    return "".join(cards)

def build_kit_email_html(payload: dict) -> str:
    total = payload.get("total_bookings", 0)
    report_date = html.escape(payload.get("report_date", ""))
    arrests_date = html.escape(payload.get("arrests_date", ""))
    report_date_display = html.escape(payload.get("report_date_display", ""))
    top_charge = html.escape(payload.get("top_charge", "N/A"))
    bookings = payload.get("bookings", [])
    charge_mix = payload.get("charge_mix", [])
    cities = payload.get("cities", [])

    booking_cards = build_kit_booking_cards(bookings)
    charge_rows = build_email_bar_rows(charge_mix, "label")
    city_rows = build_email_bar_rows(cities, "city")

    cta_button = f'''
    <table role="presentation" align="center" cellpadding="0" cellspacing="0" border="0" style="margin:28px auto;">
      <tr>
        <td align="center" bgcolor="#1f1f1f" style="border-radius:8px;">
          <a href="{html.escape(SUBSCRIBER_REPORT_URL)}" style="display:inline-block; padding:16px 28px; font-family:Arial, Helvetica, sans-serif; color:#ffffff; text-decoration:none; font-size:16px; font-weight:800; border-radius:8px;">View Full Report</a>
        </td>
      </tr>
    </table>
    '''

    return f'''<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Tarrant County Jail Report</title>
</head>
<body style="margin:0; padding:0; background:#f4f1eb;">
  <center style="width:100%; background:#f4f1eb;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f4f1eb;">
      <tr>
        <td align="center" style="padding:28px 12px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:680px; background:#ffffff; border-left:1px solid #e6e0d6; border-right:1px solid #e6e0d6;">
            <tr>
              <td style="height:6px; background:linear-gradient(to right, #2a2a2a 0%, #2a2a2a 70%, #c8a45a 70%, #c8a45a 100%); font-size:1px; line-height:1px;">&nbsp;</td>
            </tr>
            <tr>
              <td style="padding:34px 28px 24px 28px;">
                <div style="display:inline-block; border:1px solid #cfc8bd; padding:8px 16px; font-family:Arial, Helvetica, sans-serif; color:#8c8780; font-size:11px; letter-spacing:4px; text-transform:uppercase; font-weight:700;">
                  Daily Jail Report - Tarrant County, TX
                </div>

                <h1 style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:38px; line-height:1.05; margin:28px 0 8px 0; font-weight:800;">
                  Tarrant County Jail Report
                </h1>
                <div style="font-family:Georgia, 'Times New Roman', serif; color:#8c8780; font-size:22px; line-height:1.3; margin-bottom:26px;">
                  {report_date_display}
                </div>

                <div style="border-top:1px solid #d7d1c8; margin:0 0 20px 0;"></div>

                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                  <tr>
                    <td style="background:#2a2a2a; color:#ffffff; font-family:Arial, Helvetica, sans-serif; font-size:12px; letter-spacing:3px; text-transform:uppercase; font-weight:800; padding:12px 14px; border-radius:4px;">
                      Unclassified // For Informational Use Only
                    </td>
                  </tr>
                </table>

                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:24px; border:1px solid #ded8cf;">
                  <tr>
                    <td width="33.33%" align="center" style="padding:20px 8px; border-right:1px solid #ded8cf;">
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:11px; letter-spacing:4px; text-transform:uppercase; font-weight:700;">Total Bookings</div>
                      <div style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:42px; line-height:1; font-weight:800; margin-top:10px;">{total}</div>
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:13px; margin-top:6px;">Last 24 Hours</div>
                    </td>
                    <td width="33.33%" align="center" style="padding:20px 8px; border-right:1px solid #ded8cf;">
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:11px; letter-spacing:4px; text-transform:uppercase; font-weight:700;">Report Date</div>
                      <div style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:30px; line-height:1; font-weight:800; margin-top:12px;">{report_date}</div>
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:13px; margin-top:8px;">Published</div>
                    </td>
                    <td width="33.33%" align="center" style="padding:20px 8px;">
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:11px; letter-spacing:4px; text-transform:uppercase; font-weight:700;">Arrests Date</div>
                      <div style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:30px; line-height:1; font-weight:800; margin-top:12px;">{arrests_date}</div>
                      <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:13px; margin-top:8px;">Booking Period</div>
                    </td>
                  </tr>
                </table>

                {cta_button}

                <h2 style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:28px; line-height:1.2; margin:34px 0 8px 0;">Daily Snapshot</h2>
                <div style="font-family:Arial, Helvetica, sans-serif; color:#88837c; font-size:15px; line-height:1.5; margin-bottom:22px;">
                  Statistical summary for the 24-hour period ending {arrests_date}.
                </div>

                <div style="background:#2a2a2a; border-radius:5px; padding:20px 22px; margin-bottom:28px;">
                  <div style="font-family:Arial, Helvetica, sans-serif; color:#c8a45a; font-size:11px; letter-spacing:3px; text-transform:uppercase; font-weight:800; margin-bottom:10px;">Top Charge Today</div>
                  <div style="font-family:Arial, Helvetica, sans-serif; color:#ffffff; font-size:18px; line-height:1.35; font-weight:800;">{top_charge}</div>
                </div>

                <h3 style="font-family:Arial, Helvetica, sans-serif; color:#8c8780; font-size:12px; letter-spacing:4px; text-transform:uppercase; margin:28px 0 12px 0;">Charge Mix</h3>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                  {charge_rows}
                </table>

                <h3 style="font-family:Arial, Helvetica, sans-serif; color:#8c8780; font-size:12px; letter-spacing:4px; text-transform:uppercase; margin:34px 0 12px 0;">Arrests by City</h3>
                <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0">
                  {city_rows}
                </table>

                <div style="border-top:3px solid #2a2a2a; margin:38px 0 30px 0;"></div>

                <h2 style="font-family:Georgia, 'Times New Roman', serif; color:#1a1a1a; font-size:34px; line-height:1.15; margin:0 0 10px 0;">Full Booking List</h2>
                <div style="font-family:Arial, Helvetica, sans-serif; color:#88837c; font-size:16px; line-height:1.5; margin-bottom:22px;">
                  Complete list of individuals booked during the reporting period.
                </div>

                {booking_cards}

                {cta_button}

                <div style="font-family:Arial, Helvetica, sans-serif; color:#999590; font-size:12px; line-height:1.5; margin-top:28px; border-top:1px solid #e6e0d6; padding-top:18px;">
                  This report is generated from publicly available data provided by Tarrant County, Texas. Booking records reflect arrests and charges at the time of booking and do not imply guilt or conviction. Individuals are presumed innocent until proven guilty in a court of law.
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </center>
</body>
</html>'''

# ---------------------------------------------------------------------------
# PDF generation
# ---------------------------------------------------------------------------

async def generate_pdf_from_html(html_content: str):
    print("Generating PDF from HTML...")
    browser = None
    try:
        browser = await launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False,
        )
        page = await browser.newPage()
        await page.setContent(html_content)
        await page.pdf({
            "path": PDF_OUTPUT_PATH,
            "format": "Letter",
            "printBackground": True,
            "margin": {"top": "0.5in", "right": "0.5in", "bottom": "0.5in", "left": "0.5in"},
        })
        print("PDF exists?", os.path.exists(PDF_OUTPUT_PATH))
        if os.path.exists(PDF_OUTPUT_PATH):
            print("PDF size:", os.path.getsize(PDF_OUTPUT_PATH), "bytes")
            if os.path.getsize(PDF_OUTPUT_PATH) == 0:
                print("WARNING: PDF size is 0 bytes.")
    except Exception as e:
        print(f"ERROR: PDF generation failed: {e}")
    finally:
        if browser:
            try:
                await browser.close()
            except Exception as close_error:
                print(f"WARNING: Browser close failed: {close_error}")

# ---------------------------------------------------------------------------
# Email to personal address
# ---------------------------------------------------------------------------

def send_email(subject: str, html_body: str):
    if not all([TO_EMAIL, SMTP_USER, SMTP_PASS]):
        print("WARNING: Missing TO_EMAIL/SMTP_USER/SMTP_PASS. Skipping email.")
        return

    print(f"Sending email to {TO_EMAIL} ...")

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("Your email client does not support HTML.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    if os.path.exists(PDF_OUTPUT_PATH):
        with open(PDF_OUTPUT_PATH, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
        pdf_attachment.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(PDF_OUTPUT_PATH)}"'
        )
        msg.attach(pdf_attachment)
        print(f"Attached PDF: {PDF_OUTPUT_PATH}")
    else:
        print(f"WARNING: PDF not found at {PDF_OUTPUT_PATH}. Email will be HTML-only.")

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())
        print("Email sent.")
    except Exception as e:
        print(f"FATAL: Email failed: {e}")

# ---------------------------------------------------------------------------
# Kit Broadcast Draft
# ---------------------------------------------------------------------------

def create_kit_broadcast(subject: str, html_body: str, preview_text: str):
    if not KIT_API_KEY:
        print("WARNING: Missing KIT_API_KEY. Skipping Kit broadcast draft.")
        return

    print("Creating Kit broadcast draft...")

    payload = {
        "subject": subject,
        "preview_text": preview_text,
        "description": subject,
        "content": html_body,
        "public": False,
        "send_at": None,
    }

    if KIT_EMAIL_TEMPLATE_ID:
        try:
            payload["email_template_id"] = int(KIT_EMAIL_TEMPLATE_ID)
        except ValueError:
            print(f"WARNING: Invalid KIT_EMAIL_TEMPLATE_ID='{KIT_EMAIL_TEMPLATE_ID}'. Using Kit default template.")

    try:
        r = requests.post(
            "https://api.kit.com/v4/broadcasts",
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "X-Kit-Api-Key": KIT_API_KEY,
            },
            json=payload,
            timeout=60,
        )

        print("Kit broadcast status:", r.status_code)
        print("Kit response:", r.text[:1000])
        r.raise_for_status()
        print("Kit broadcast draft created successfully.")

    except Exception as e:
        print(f"ERROR: Kit broadcast draft failed: {e}")

# ---------------------------------------------------------------------------
# Base44 Sync
# ---------------------------------------------------------------------------

def send_report_to_base44(report_payload: dict):
    if not BASE44_FUNCTION_URL:
        print("WARNING: Missing BASE44_FUNCTION_URL. Skipping Base44 sync.")
        return

    if not BASE44_AUTOMATION_API_KEY:
        print("WARNING: Missing BASE44_AUTOMATION_API_KEY. Skipping Base44 sync.")
        return

    print("Sending latest report data to Base44...")

    try:
        response = requests.post(
            BASE44_FUNCTION_URL,
            headers={
                "Content-Type": "application/json",
                "x-automation-api-key": BASE44_AUTOMATION_API_KEY,
            },
            json=report_payload,
            timeout=60,
        )

        print("Base44 sync status:", response.status_code)
        print("Base44 response:", response.text[:1000])
        response.raise_for_status()
        print("Base44 report sync completed successfully.")

    except Exception as e:
        print(f"ERROR: Base44 report sync failed: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    print("--- Starting Tarrant County Daily Jail Report ---")

    pdf_url = f"{BOOKED_BASE_URL.rstrip('/')}/{BOOKED_DAY}.PDF"
    pdf_bytes = fetch_pdf(pdf_url)

    report_dt, records = parse_booked_in(pdf_bytes)
    records = fix_embedded_booking_numbers(records)

    stats = analyze_stats(records)

    def fmt(dt: datetime, fmt_str: str, fallback: str):
        try:
            return dt.strftime(fmt_str)
        except Exception:
            return dt.strftime(fallback)

    report_date_str = fmt(report_dt, "%-m/%-d/%Y", "%m/%d/%Y")
    arrests_date_str = fmt(report_dt - timedelta(days=1), "%-m/%-d/%Y", "%m/%d/%Y")
    report_date_display = fmt(report_dt, "%A, %B %-d, %Y", "%A, %B %d, %Y")

    sorted_records = sorted(records, key=lambda x: x.get("name", ""))

    template_data = {
        **stats,
        "report_date": report_date_str,
        "arrests_date": arrests_date_str,
        "report_date_display": report_date_display,
        "bookings": sorted_records,
    }

    structured_payload = build_structured_payload(
        stats=stats,
        records=records,
        report_date_str=report_date_str,
        arrests_date_str=arrests_date_str,
        report_date_display=report_date_display,
    )
    save_json_payload(structured_payload)

    full_html_content = render_html(template_data)
    await generate_pdf_from_html(full_html_content)

    # Use plain hyphen to avoid broken Unicode in Kit subject lines.
    subject = f"Tarrant County Jail Report - Arrests for {arrests_date_str}"

    # Existing daily personal email keeps full report + PDF attachment.
    send_email(subject, full_html_content)

    # Base44 live website update stays automated.
    send_report_to_base44(structured_payload)

    # Kit draft uses mobile-friendly email HTML with ALL bookings as cards.
    kit_html_content = build_kit_email_html(structured_payload)
    create_kit_broadcast(
        subject=subject,
        html_body=kit_html_content,
        preview_text=f"Arrests booked on {arrests_date_str}",
    )

    print("--- Done ---")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    finally:
        loop.close()