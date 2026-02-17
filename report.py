"""
# ---------------------------------------------------------------------------
# Tarrant County Daily Jail Report
#
# This script automates the generation of a professional daily jail report
# for Tarrant County, TX. It performs the following steps:
#
# 1.  **Fetches Data**: Scrapes the latest "booked-in" PDF report from the
#     Tarrant County Criminal Justice Reports website.
# 2.  **Parses Data**: Extracts and cleans all booking records from the PDF,
#     preserving the battle-tested parsing logic from the original implementation.
# 3.  **Analyzes Stats**: Calculates key statistics for the daily snapshot,
#     such as total bookings, top charge, charge mix, and city breakdown.
# 4.  **Generates HTML**: Populates a professional HTML template with the
#     scraped data and calculated stats.
# 5.  **Generates PDF**: Uses a headless browser (Pyppeteer/Chromium) to create a
#     high-quality, pixel-perfect PDF version of the report from the HTML.
# 6.  **Sends Email**: Sends an email containing the HTML report in the body
#     and the generated PDF as an attachment.
# 7.  **Sends Kit Broadcast**: Creates and sends a Kit (ConvertKit) broadcast
#     to all subscribers via the Kit API v3, using the generated HTML report
#     as the email content.
#
# This script is designed to be run via a GitHub Actions workflow on a
# daily schedule.
# ---------------------------------------------------------------------------
"""

import os
import re
import ssl
import json
import smtplib
import asyncio
import html
from io import BytesIO
from datetime import datetime, timedelta, timezone
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pdfplumber
import requests
from pyppeteer import launch

# ---------------------------------------------------------------------------
# Constants & Configuration
# ---------------------------------------------------------------------------

# --- Environment-based Configuration ---
BOOKED_BASE_URL = os.getenv("BOOKED_BASE_URL", "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF")
BOOKED_DAY = os.getenv("BOOKED_DAY", "01")
TO_EMAIL = os.getenv("TO_EMAIL")
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))

# --- Kit (ConvertKit) API Configuration ---
KIT_API_SECRET = os.getenv("KIT_API_SECRET")
KIT_API_BASE_URL = "https://api.kit.com/v3"
KIT_FROM_EMAIL = "report@dailyjailreports.com"

# --- File Paths ---
HTML_TEMPLATE_PATH = "daily_report_template.html"
HTML_OUTPUT_PATH = "daily_jail_report.html"
PDF_OUTPUT_PATH = "daily_jail_report.pdf"

# --- PDF Parsing & Cleaning Patterns (Preserved from original implementation) ---
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

# --- Charge Categorization Rules ---
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
# PDF Scraping & Parsing (Logic preserved from original implementation)
# ---------------------------------------------------------------------------

def fetch_pdf(url: str) -> bytes:
    """Fetches the PDF content from a given URL."""
    print(f"Fetching PDF from {url}...")
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        print("Successfully fetched PDF.")
        return r.content
    except requests.RequestException as e:
        print(f"FATAL: Error fetching PDF from {url}: {e}")
        raise

def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def is_junk_line(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up: return True
    return any(s in up for s in JUNK_SUBSTRINGS)

def looks_like_address(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up: return False
    if CITY_STATE_ZIP_RE.match(up) or CITY_STATE_RE.match(up): return True
    if LEADING_STREET_NUM_RE.match(up): return True
    return STREET_SUFFIX_RE.search(up) is not None

def clean_charge_line(raw: str) -> str:
    if not raw: return ""
    s = normalize_ws(raw)
    if is_junk_line(s): return ""
    s = INLINE_STREET_ADDR_RE.sub("", s).strip()
    s = TRAILING_CITY_TX_ZIP_RE.sub("", s).strip()
    s = re.sub(r"\s+TX\s+\d{5}(?:-\d{4})?\s*$", "", s).strip()
    return s

def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_ZIP_RE.match(up)
        if m: return normalize_ws(m.group("city").title())
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m = CITY_STATE_RE.match(up)
        if m: return normalize_ws(m.group("city").title())
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m2 = re.search(r"([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?$", up)
        if m2: return normalize_ws(m2.group(1).title())
        m3 = re.search(r"\b([A-Z][A-Z \-']+),?\s+TX\s+\d{5}(?:-\d{4})?\b", up)
        if m3: return normalize_ws(m3.group(1).title())
    return "Unknown"

def apply_content_line(rec: dict, ln: str) -> None:
    """
    Processes a content line and appends it to the appropriate field of the record.
    FIX (Issue 2): Before processing, check if the line starts with a booking number
    pattern that should NOT be appended to the name. Also handles inline booking
    numbers correctly.
    """
    rec.setdefault("addr_lines", [])
    rec.setdefault("charges", [])
    s = normalize_ws(ln)
    if not s or is_junk_line(s): return

    bookings = list(BOOKING_RE.finditer(s))
    if bookings:
        pre = s[: bookings[0].start()].strip()
        if pre and looks_like_address(pre):
            rec["addr_lines"].append(pre)
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(s)
            chunk_clean = clean_charge_line(s[start:end].strip(" -\t"))
            if chunk_clean: rec["charges"].append(chunk_clean)
        return

    if looks_like_address(s):
        rec["addr_lines"].append(s)
        return

    cleaned = clean_charge_line(s)
    if not cleaned: return
    if not rec["charges"]: rec["charges"].append(cleaned)
    else: rec["charges"][-1] = normalize_ws(rec["charges"][-1] + " " + cleaned)


def _split_name_with_embedded_booking(name_raw: str) -> tuple:
    """
    FIX (Issue 2): Checks if a name field contains an embedded booking number
    pattern (XX-XXXXXXX) and subsequent charge text. If found, splits the name
    at that point.

    Returns:
        (clean_name, extra_charges_list)
        - clean_name: the actual name (everything before the booking number)
        - extra_charges_list: list of charge strings extracted from after the booking number(s)
    """
    if not name_raw:
        return (name_raw, [])

    m = BOOKING_RE.search(name_raw)
    if not m:
        return (name_raw.strip(), [])

    # Everything before the booking number is the real name
    clean_name = name_raw[:m.start()].strip()

    # Everything from the booking number onward may contain charges
    remainder = name_raw[m.start():]
    extra_charges = []
    bookings = list(BOOKING_RE.finditer(remainder))
    for i, b in enumerate(bookings):
        start = b.end()
        end = bookings[i + 1].start() if i + 1 < len(bookings) else len(remainder)
        chunk = remainder[start:end].strip(" -\t")
        cleaned = clean_charge_line(chunk)
        if cleaned:
            extra_charges.append(cleaned)

    return (clean_name, extra_charges)


def finalize_record(rec: dict) -> dict:
    """
    Finalizes a parsed record, cleaning up charges and extracting city info.
    FIX (Issue 2): Post-processes the name field to detect and split out any
    embedded booking numbers and charge text that were incorrectly appended.
    """
    # --- FIX (Issue 2): Split name if it contains embedded booking number ---
    raw_name = rec.get("name", "").strip()
    clean_name, extra_charges = _split_name_with_embedded_booking(raw_name)

    charges = rec.get("charges", [])
    # Prepend any charges extracted from the name field
    if extra_charges:
        charges = extra_charges + charges

    # Deduplicate and clean charges
    seen = set()
    unique_charges = []
    for c in charges:
        cleaned = clean_charge_line(c)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            unique_charges.append(cleaned)

    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]
    return {
        "name": clean_name,
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": extract_city_from_addr_lines(addr_lines),
        "description": ", ".join(unique_charges),
    }

def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    """Parses the raw PDF bytes and extracts structured booking records."""
    records: list[dict] = []
    pending = None
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        try:
            first_page_text = pdf.pages[0].extract_text() or ""
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", first_page_text)
            report_dt = datetime.strptime(m.group(1), "%m/%d/%Y") if m else datetime.now()
        except (ValueError, IndexError, AttributeError):
            report_dt = datetime.now()

        for page in pdf.pages:
            lines = (page.extract_text(x_tolerance=2, y_tolerance=2) or "").splitlines()
            for ln in [l.strip() for l in lines if l.strip()]:
                if is_junk_line(ln): continue
                mA = NAME_CID_DATE_RE.match(ln)
                if mA:
                    if current: records.append(finalize_record(current))
                    current = {"name": mA.group("name"), "cid": mA.group("cid"), "book_in_date": mA.group("date"), "addr_lines": [], "charges": []}
                    pending = None
                    continue
                mB = CID_DATE_ONLY_RE.match(ln)
                if mB:
                    if current: records.append(finalize_record(current))
                    current = None
                    pending = (mB.group("cid"), mB.group("date"))
                    continue
                if pending and NAME_ONLY_RE.match(ln):
                    current = {"name": ln, "cid": pending[0], "book_in_date": pending[1], "addr_lines": [], "charges": []}
                    pending = None
                    continue
                if pending and not current and ln: pending = None
                if current: apply_content_line(current, ln)
        if current: records.append(finalize_record(current))
    
    print(f"Successfully parsed {len(records)} booking records.")
    return report_dt, records

# ---------------------------------------------------------------------------
# Stats Analysis
# ---------------------------------------------------------------------------

def categorize_charge(charge_text: str) -> str:
    """Categorizes a charge based on keywords."""
    up = charge_text.upper()
    for cat_name, keywords in CATEGORY_RULES:
        if any(kw in up for kw in keywords):
            return cat_name
    return "Other"

def analyze_stats(records: list[dict]) -> dict:
    """Analyzes booking records and returns statistics."""
    total = len(records)
    if total == 0:
        return {
            "total_bookings": 0,
            "top_charge": "N/A",
            "top_charge_count": 0,
            "charge_mix": [],
            "city_breakdown": [],
        }

    # Flatten all charges
    all_charges = []
    for rec in records:
        desc = rec.get("description", "")
        if desc:
            all_charges.extend([c.strip() for c in desc.split(",") if c.strip()])

    # Categorize charges
    categorized = [categorize_charge(c) for c in all_charges]
    charge_counts = Counter(categorized)
    top_charge = charge_counts.most_common(1)[0] if charge_counts else ("N/A", 0)

    # Charge mix — ALL categories (sorted by count descending)
    charge_mix = [{"category": cat, "count": cnt} for cat, cnt in charge_counts.most_common()]

    # City breakdown — top 9 cities + "All Other Cities" aggregate
    cities = [rec.get("city", "Unknown") for rec in records]
    city_counts = Counter(cities)
    top_9 = city_counts.most_common(9)
    top_9_total = sum(cnt for _, cnt in top_9)
    other_total = total - top_9_total
    city_breakdown = [{"city": city, "count": cnt} for city, cnt in top_9]
    if other_total > 0:
        city_breakdown.append({"city": "All Other Cities", "count": other_total})

    return {
        "total_bookings": total,
        "top_charge": top_charge[0],
        "top_charge_count": top_charge[1],
        "charge_mix": charge_mix,
        "city_breakdown": city_breakdown,
    }

# ---------------------------------------------------------------------------
# Email-Safe HTML Builders (v4 — Percentage-based bars & refined styling)
# ---------------------------------------------------------------------------

# Color palette for bar charts
BAR_COLOR_PRIMARY = "#c8a45a"
BAR_COLOR_ALT = "#2c2c2c"
BAR_BG = "#e8e4dc"  # Changed from #f0ede6 — warmer beige
LABEL_COLOR = "#5c5955"  # Changed from #2c2c2c — warm medium gray for labels
COUNT_COLOR = "#999590"
LABEL_FONT = "Georgia, 'Times New Roman', Times, serif"  # Serif font for labels
FONT_STACK = "Arial, Helvetica, sans-serif"  # Keep for non-label elements


def build_charge_mix_bars(charge_mix: list[dict]) -> str:
    """
    Builds email-safe HTML bar chart rows for the Charge Mix section.
    v4: Uses percentage-based two-cell bar tables for universal email client support.
    Shows ALL categories with gold (#c8a45a) bars proportional to the
    largest value. Each row shows: Label | [gold bar][beige remainder] | XX% (count)
    Percentage is bold, count is in lighter color with parentheses.
    """
    if not charge_mix:
        return ""

    total_charges = sum(item["count"] for item in charge_mix)
    max_count = max(item["count"] for item in charge_mix) if charge_mix else 1
    if max_count <= 0:
        max_count = 1
    rows_html = ""

    for item in charge_mix:
        category = html.escape(item["category"])
        count = item["count"]
        pct = round((count / total_charges) * 100) if total_charges > 0 else 0
        # Scale relative to max — top item gets 100%, others proportional
        ratio = count / max_count
        bar_pct = max(int(ratio * 100), 2)  # minimum 2% for visibility
        bg_pct = 100 - bar_pct

        rows_html += (
            '<tr>\n'
            f'  <td style="padding:10px 10px 10px 0; font-family:{LABEL_FONT}; font-size:13px; '
            f'color:{LABEL_COLOR}; white-space:nowrap; vertical-align:middle;" '
            f'align="left">{category}</td>\n'
            f'  <td style="padding:10px 0; vertical-align:middle;" width="50%">\n'
            f'    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
            f'<tr>'
            f'<td width="{bar_pct}%" style="background-color:{BAR_COLOR_PRIMARY}; height:14px; font-size:1px; line-height:1px;" bgcolor="{BAR_COLOR_PRIMARY}">&nbsp;</td>'
            f'<td width="{bg_pct}%" style="background-color:{BAR_BG}; height:14px; font-size:1px; line-height:1px;" bgcolor="{BAR_BG}">&nbsp;</td>'
            f'</tr></table>\n'
            f'  </td>\n'
            f'  <td style="padding:10px 0 10px 10px; font-family:{LABEL_FONT}; font-size:13px; '
            f'white-space:nowrap; vertical-align:middle;" '
            f'align="right"><strong style="color:#2c2c2c;">{pct}%</strong> '
            f'<span style="color:{COUNT_COLOR};">({count})</span></td>\n'
            '</tr>\n'
        )

    return rows_html


def build_city_bars(city_breakdown: list[dict], total_bookings: int) -> str:
    """
    Builds email-safe HTML bar chart rows for the Arrests by City section.
    v4: Uses percentage-based two-cell bar tables for universal email client support.
    Shows top 9 cities + "All Other Cities" with dark (#2c2c2c) bars
    proportional to the largest value. Each row shows:
    Label | [dark bar][beige remainder] | XX% (count)
    Percentage is bold, count is in lighter color with parentheses.
    "All Other Cities" row is in italics.
    Percentages are of TOTAL bookings.
    """
    if not city_breakdown:
        return ""

    max_count = max(item["count"] for item in city_breakdown) if city_breakdown else 1
    if max_count <= 0:
        max_count = 1
    if total_bookings <= 0:
        total_bookings = 1
    rows_html = ""

    for item in city_breakdown:
        city_name = html.escape(item["city"])
        count = item["count"]
        pct = round((count / total_bookings) * 100) if total_bookings > 0 else 0
        ratio = count / max_count
        bar_pct = max(int(ratio * 100), 2)  # minimum 2% for visibility
        bg_pct = 100 - bar_pct

        # "All Other Cities" row in italics
        is_other = (item["city"] == "All Other Cities")
        label_open = '<em>' if is_other else ''
        label_close = '</em>' if is_other else ''

        rows_html += (
            '<tr>\n'
            f'  <td style="padding:10px 10px 10px 0; font-family:{LABEL_FONT}; font-size:13px; '
            f'color:{LABEL_COLOR}; white-space:nowrap; vertical-align:middle;" '
            f'align="left">{label_open}{city_name}{label_close}</td>\n'
            f'  <td style="padding:10px 0; vertical-align:middle;" width="50%">\n'
            f'    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
            f'<tr>'
            f'<td width="{bar_pct}%" style="background-color:{BAR_COLOR_ALT}; height:14px; font-size:1px; line-height:1px;" bgcolor="{BAR_COLOR_ALT}">&nbsp;</td>'
            f'<td width="{bg_pct}%" style="background-color:{BAR_BG}; height:14px; font-size:1px; line-height:1px;" bgcolor="{BAR_BG}">&nbsp;</td>'
            f'</tr></table>\n'
            f'  </td>\n'
            f'  <td style="padding:10px 0 10px 10px; font-family:{LABEL_FONT}; font-size:13px; '
            f'white-space:nowrap; vertical-align:middle;" '
            f'align="right">{label_open}<strong style="color:#2c2c2c;">{pct}%</strong> '
            f'<span style="color:{COUNT_COLOR};">({count})</span>{label_close}</td>\n'
            '</tr>\n'
        )

    return rows_html


# Abbreviated labels for Charge Distribution section
CATEGORY_ABBREVIATIONS = {
    "Family Violence / Assault": "Fam. Violence",
    "DWI / Alcohol": "DWI / Alcohol",
    "Drugs / Possession": "Drugs / Poss.",
    "Theft / Fraud": "Theft / Fraud",
    "Weapons": "Weapons",
    "Evading / Resisting": "Evading",
    "Warrants / Court / Bond": "Warrants",
    "Other": "Other",
}


def build_charge_distribution_bars(charge_mix: list[dict], total_charges: int) -> str:
    """
    Builds email-safe HTML bar chart rows for the Charge Distribution section.
    v4: Uses percentage-based two-cell bar tables for universal email client support.
    Shows ALL categories with abbreviated labels. Bars alternate between
    gold (#c8a45a) and dark (#2c2c2c) colors. Percentage only (no count).
    Bars are proportional to the largest percentage.
    """
    if not charge_mix or total_charges == 0:
        return ""

    # Alternate between gold and dark
    alt_colors = [BAR_COLOR_PRIMARY, BAR_COLOR_ALT]

    # Pre-calculate percentages
    items_with_pct = []
    for item in charge_mix:
        pct = round((item["count"] / total_charges) * 100) if total_charges > 0 else 0
        items_with_pct.append((item, pct))

    # Find the maximum percentage for relative scaling
    max_pct = max(pct for _, pct in items_with_pct) if items_with_pct else 1
    if max_pct <= 0:
        max_pct = 1

    rows_html = ""

    for idx, (item, pct_of_total) in enumerate(items_with_pct):
        full_name = item["category"]
        abbrev = CATEGORY_ABBREVIATIONS.get(full_name, full_name)
        category = html.escape(abbrev)
        color = alt_colors[idx % 2]

        # Scale relative to the largest percentage — top item gets 100%, others proportional
        ratio = pct_of_total / max_pct if max_pct > 0 else 0
        bar_pct = max(int(ratio * 100), 2)  # minimum 2% for visibility
        bg_pct = 100 - bar_pct

        rows_html += (
            '<tr>\n'
            f'  <td style="padding:10px 10px 10px 0; font-family:{LABEL_FONT}; font-size:13px; '
            f'color:{LABEL_COLOR}; white-space:nowrap; vertical-align:middle;" '
            f'align="left">{category}</td>\n'
            f'  <td style="padding:10px 0; vertical-align:middle;" width="50%">\n'
            f'    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;">'
            f'<tr>'
            f'<td width="{bar_pct}%" style="background-color:{color}; height:14px; font-size:1px; line-height:1px;" bgcolor="{color}">&nbsp;</td>'
            f'<td width="{bg_pct}%" style="background-color:{BAR_BG}; height:14px; font-size:1px; line-height:1px;" bgcolor="{BAR_BG}">&nbsp;</td>'
            f'</tr></table>\n'
            f'  </td>\n'
            f'  <td style="padding:10px 0 10px 10px; font-family:{LABEL_FONT}; font-size:13px; '
            f'font-weight:700; color:#2c2c2c; white-space:nowrap; vertical-align:middle;" '
            f'align="right">{pct_of_total}%</td>\n'
            '</tr>\n'
        )

    return rows_html


def build_bookings_table(bookings: list[dict]) -> str:
    """
    Builds email-safe HTML table rows for the Full Booking List.
    Uses simple inline-styled <tr>/<td> elements with alternating row colors.
    All styles are inline. No CSS classes.
    FIX (Issue 3): Clean, consistent column widths and improved padding.
    """
    if not bookings:
        return ""

    rows_html = ""
    for idx, booking in enumerate(bookings):
        row_num = idx + 1
        name = html.escape(booking.get("name", ""))
        date = html.escape(booking.get("book_in_date", ""))
        charges = html.escape(booking.get("description", ""))
        city = html.escape(booking.get("city", ""))

        # Alternating row background
        bg = "#ffffff" if row_num % 2 == 1 else "#f9f8f6"

        # Common cell style — consistent padding and font
        cell_base = (
            f"padding:9px 10px; font-family:{FONT_STACK}; font-size:11px; "
            f"color:#2c2c2c; border-bottom:1px solid #e8e4dc; vertical-align:top;"
        )

        rows_html += (
            f'<tr style="background-color:{bg};" bgcolor="{bg}">\n'
            f'  <td style="{cell_base} white-space:nowrap; color:{COUNT_COLOR};" align="center" width="5%">{row_num}</td>\n'
            f'  <td style="{cell_base} font-weight:700;" align="left" width="22%">{name}</td>\n'
            f'  <td style="{cell_base} white-space:nowrap;" align="left" width="12%">{date}</td>\n'
            f'  <td style="{cell_base}" align="left" width="46%">{charges}</td>\n'
            f'  <td style="{cell_base} white-space:nowrap;" align="left" width="15%">{city}</td>\n'
            '</tr>\n'
        )

    return rows_html


# ---------------------------------------------------------------------------
# HTML Report Generation
# ---------------------------------------------------------------------------

def render_html(data: dict) -> str:
    """Renders the HTML report using the template and data."""
    try:
        with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            template = f.read()
    except FileNotFoundError:
        print(f"FATAL: Template file not found at {HTML_TEMPLATE_PATH}")
        raise

    # Build email-safe HTML for bar charts and booking table
    charge_mix_html = build_charge_mix_bars(data["charge_mix"])
    city_html = build_city_bars(data["city_breakdown"], data["total_bookings"])

    # Calculate total charges for distribution percentages
    total_charges = sum(item["count"] for item in data["charge_mix"])
    bar_html = build_charge_distribution_bars(data["charge_mix"], total_charges)

    bookings_html = build_bookings_table(data["bookings"])

    # Replace placeholders
    html_output = template.replace("{{report_date}}", data["report_date"])
    html_output = html_output.replace("{{arrests_date}}", data["arrests_date"])
    html_output = html_output.replace("{{report_date_display}}", data["report_date_display"])
    html_output = html_output.replace("{{total_bookings}}", str(data["total_bookings"]))
    html_output = html_output.replace("{{top_charge}}", html.escape(data["top_charge"]))
    html_output = html_output.replace("{{top_charge_count}}", str(data["top_charge_count"]))
    html_output = html_output.replace("{{charge_mix_rows}}", charge_mix_html)
    html_output = html_output.replace("{{city_rows}}", city_html)
    html_output = html_output.replace("{{bar_rows}}", bar_html)
    html_output = html_output.replace("{{booking_rows}}", bookings_html)

    # Save the HTML output
    with open(HTML_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html_output)
    print(f"HTML report saved to {HTML_OUTPUT_PATH}")

    return html_output

# ---------------------------------------------------------------------------
# PDF Generation (Using Pyppeteer)
# ---------------------------------------------------------------------------

async def generate_pdf_from_html(html_content: str):
    """Generates a PDF from HTML content using Pyppeteer."""
    print("Generating PDF from HTML...")
    try:
        browser = await launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
        page = await browser.newPage()
        await page.setContent(html_content)
        # Wait for rendering to complete
        await page.waitFor(2000)
        await page.pdf({
            "path": PDF_OUTPUT_PATH,
            "format": "Letter",
            "printBackground": True,
            "margin": {"top": "0.5in", "right": "0.5in", "bottom": "0.5in", "left": "0.5in"},
        })
        await browser.close()
        print(f"PDF report saved to {PDF_OUTPUT_PATH}")
    except Exception as e:
        print(f"ERROR: Failed to generate PDF: {e}")
        # Don't raise — allow email and Kit broadcast to proceed even if PDF fails
        print("WARNING: Continuing without PDF attachment.")

# ---------------------------------------------------------------------------
# Email Sending
# ---------------------------------------------------------------------------

def send_email(subject: str, html_body: str):
    """Sends an email with the HTML report and PDF attachment."""
    if not all([TO_EMAIL, SMTP_USER, SMTP_PASS]):
        print("WARNING: Email credentials not fully configured. Skipping email send.")
        return

    print(f"Sending email to {TO_EMAIL}...")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL

    # Attach HTML body
    msg.attach(MIMEText(html_body, "html"))

    # Attach PDF
    try:
        with open(PDF_OUTPUT_PATH, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
            pdf_attachment.add_header("Content-Disposition", "attachment", filename=PDF_OUTPUT_PATH)
            msg.attach(pdf_attachment)
    except FileNotFoundError:
        print(f"WARNING: PDF file not found at {PDF_OUTPUT_PATH}. Sending email without attachment.")

    # Send email
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, TO_EMAIL, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"ERROR: Failed to send email: {e}")
        # Don't raise, just log the error.

# ---------------------------------------------------------------------------
# Kit (ConvertKit) Broadcast Sending - V3 API
# ---------------------------------------------------------------------------

def _strip_html_wrapper_for_kit(html_body: str) -> str:
    """
    Strips the <!DOCTYPE html>, <html>, <head>, and <body> wrapper tags from
    the HTML content so that Kit (ConvertKit) doesn't render them as visible
    text in the broadcast email. Returns only the inner body content.
    """
    import re as _re
    # Extract content between <body...> and </body>
    body_match = _re.search(r'<body[^>]*>(.*)</body>', html_body, _re.DOTALL | _re.IGNORECASE)
    if body_match:
        return body_match.group(1).strip()
    # Fallback: strip DOCTYPE, html, head tags manually
    stripped = _re.sub(r'<!DOCTYPE[^>]*>', '', html_body, flags=_re.IGNORECASE).strip()
    stripped = _re.sub(r'</?html[^>]*>', '', stripped, flags=_re.IGNORECASE).strip()
    stripped = _re.sub(r'<head[^>]*>.*?</head>', '', stripped, flags=_re.IGNORECASE | _re.DOTALL).strip()
    stripped = _re.sub(r'</?body[^>]*>', '', stripped, flags=_re.IGNORECASE).strip()
    return stripped


def send_kit_broadcast(subject: str, html_body: str, report_date_str: str):
    """
    Creates and sends a Kit (ConvertKit) broadcast to ALL subscribers
    via the Kit API v3.

    Workflow:
      1. Create a broadcast with content, subject, and send_at set to current time
         (which schedules it for immediate sending).
      2. The broadcast is sent to all subscribers automatically.

    Args:
        subject:          The email subject line.
        html_body:        The full HTML content for the email body.
        report_date_str:  The report date string for logging purposes.
    """
    if not KIT_API_SECRET:
        print("WARNING: KIT_API_SECRET environment variable not set. Skipping Kit broadcast.")
        return

    print("--- Kit Broadcast: Starting ---")
    print(f"Kit Broadcast: Subject = {subject}")
    print(f"Kit Broadcast: From = {KIT_FROM_EMAIL}")
    print(f"Kit Broadcast: Target = ALL subscribers")

    # -----------------------------------------------------------------------
    # Create and send the broadcast via v3 API
    # -----------------------------------------------------------------------
    try:
        print("Kit Broadcast: Creating and sending broadcast via v3 API...")
        create_url = f"{KIT_API_BASE_URL}/broadcasts"
        headers = {
            "Content-Type": "application/json",
        }

        # Use current UTC time for immediate sending
        # v3 API: if send_at is set to current/past time, it sends immediately
        # if send_at is omitted, it creates a draft
        now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        # Build the broadcast payload
        # Note: v3 API requires api_secret in the body, not as a header
        # Strip DOCTYPE/html/head wrapper to prevent Kit from rendering
        # "DOCTYPE html>" as visible text at the top of the broadcast
        kit_content = _strip_html_wrapper_for_kit(html_body)

        payload = {
            "api_secret": KIT_API_SECRET,
            "subject": subject,
            "content": kit_content,
            "description": f"Tarrant County Jail Report — {report_date_str}",
            "email_address": KIT_FROM_EMAIL,
            "send_at": now_utc,
        }

        resp = requests.post(create_url, headers=headers, json=payload, timeout=60)

        # Log the response for debugging
        print(f"Kit Broadcast: API response status = {resp.status_code}")

        if resp.status_code == 201:
            broadcast_data = resp.json()
            broadcast_id = broadcast_data.get("broadcast", {}).get("id", "unknown")
            send_at = broadcast_data.get("broadcast", {}).get("send_at", "unknown")
            print(f"Kit Broadcast: SUCCESS! Broadcast created (ID: {broadcast_id})")
            print(f"Kit Broadcast: Scheduled send_at = {send_at}")
            print("Kit Broadcast: The broadcast will be sent to all subscribers.")
        elif resp.status_code == 401:
            print("Kit Broadcast: ERROR - Authentication failed (401). Check your KIT_API_SECRET.")
            print(f"Kit Broadcast: Response body: {resp.text}")
        elif resp.status_code == 403:
            print("Kit Broadcast: ERROR - Forbidden (403). Your Kit plan may not support this feature.")
            print(f"Kit Broadcast: Response body: {resp.text}")
        elif resp.status_code == 422:
            print("Kit Broadcast: ERROR - Validation error (422). Check the payload.")
            print(f"Kit Broadcast: Response body: {resp.text}")
        else:
            print(f"Kit Broadcast: ERROR - Unexpected status code {resp.status_code}")
            print(f"Kit Broadcast: Response body: {resp.text}")

    except requests.exceptions.Timeout:
        print("Kit Broadcast: ERROR - Request timed out. The Kit API may be slow or unreachable.")
    except requests.exceptions.ConnectionError:
        print("Kit Broadcast: ERROR - Could not connect to the Kit API. Check network connectivity.")
    except requests.exceptions.RequestException as e:
        print(f"Kit Broadcast: ERROR - Request failed: {e}")
    except json.JSONDecodeError as e:
        print(f"Kit Broadcast: ERROR - Could not parse API response as JSON: {e}")
    except Exception as e:
        print(f"Kit Broadcast: ERROR - Unexpected error: {e}")

    print("--- Kit Broadcast: Finished ---")

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------

async def main():
    """Main function to run the entire report generation process."""
    print("--- Starting Tarrant County Daily Jail Report Generation ---")
    
    # 1. Fetch and parse the PDF
    pdf_url = f"{BOOKED_BASE_URL.rstrip('/')}/{BOOKED_DAY}.PDF"
    pdf_bytes = fetch_pdf(pdf_url)
    report_dt, records = parse_booked_in(pdf_bytes)

    # 2. Analyze statistics
    stats = analyze_stats(records)

    # 3. Prepare data for the template
    report_date_str = report_dt.strftime("%-m/%-d/%Y")
    template_data = {
        **stats,
        "report_date": report_date_str,
        "arrests_date": (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y"),
        "report_date_display": report_dt.strftime("%A, %B %-d, %Y"),
        "bookings": sorted(records, key=lambda x: x.get("name", "")),
    }

    # 4. Generate HTML report
    html_content = render_html(template_data)

    # 5. Generate PDF report
    await generate_pdf_from_html(html_content)

    # 6. Send the email to the owner (existing behavior)
    subject = f"Tarrant County Jail Report — {report_date_str}"
    send_email(subject, html_content)

    # 7. Send Kit broadcast to ALL subscribers (v3 API)
    send_kit_broadcast(subject, html_content, report_date_str)

    print("--- Report generation process complete. ---")

if __name__ == "__main__":
    asyncio.run(main())
