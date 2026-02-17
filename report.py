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
#
# This script is designed to be run via a GitHub Actions workflow on a
# daily schedule.
# ---------------------------------------------------------------------------
"""

import os
import re
import ssl
import smtplib
import asyncio
import html
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

# --- Kit (ConvertKit) Configuration ---
KIT_API_KEY = os.getenv("KIT_API_KEY")
KIT_API_SECRET = os.getenv("KIT_API_SECRET")

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

# --- Name Parsing Pattern (for embedded booking numbers) ---
EMBEDDED_BOOKING_RE = re.compile(r"(\d{2}-\d{7})")

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

def finalize_record(rec: dict) -> dict:
    charges = [c for i, c in enumerate([clean_charge_line(c) for c in rec.get("charges", []) if c]) if c not in rec.get("charges", [])[:i]]
    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]
    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": extract_city_from_addr_lines(addr_lines),
        "description": ", ".join(charges),
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
# Name Parsing Fix (NEW)
# ---------------------------------------------------------------------------

def fix_embedded_booking_numbers(records: list[dict]) -> list[dict]:
    """
    Post-processing step to fix names with embedded booking numbers.
    Example: "BROWN, YARON VICTORY 26-0261822 ASSAULT CAUSES BODILY INJURY FAMILY VIOLENCE"
    becomes name="BROWN, YARON VICTORY" and the booking number + charges move to description.
    """
    print("Applying name parsing fix for embedded booking numbers...")
    fixed_records = []
    for rec in records:
        name = rec.get("name", "")
        match = EMBEDDED_BOOKING_RE.search(name)
        if match:
            # Split at the booking number
            booking_start = match.start()
            clean_name = name[:booking_start].strip()
            extra_content = name[booking_start:].strip()
            
            # Move the booking number and everything after it to the description
            existing_desc = rec.get("description", "")
            if existing_desc:
                new_desc = f"{extra_content}, {existing_desc}"
            else:
                new_desc = extra_content
            
            rec["name"] = clean_name
            rec["description"] = new_desc
        
        fixed_records.append(rec)
    
    print("Name parsing fix complete.")
    return fixed_records

# ---------------------------------------------------------------------------
# Data Analysis & Statistics
# ---------------------------------------------------------------------------

def analyze_stats(records: list[dict]) -> dict:
    """Analyzes the booking records to generate snapshot statistics."""
    total_bookings = len(records)
    if total_bookings == 0: return {"total_bookings": 0}

    # 1. Top single charge
    first_charges = [rec.get("description", "").split(",")[0].strip().upper() for rec in records if rec.get("description")]
    charge_counter = Counter(first_charges)
    top_charge = charge_counter.most_common(1)[0][0] if charge_counter else "N/A"

    # 2. Charge Mix
    categorized_charges = [(rec.get("description") or "").upper() for rec in records]
    charge_mix_counts = Counter()
    for charge_text in categorized_charges:
        found_cat = "Other / Unknown"
        for category, keywords in CATEGORY_RULES:
            if any(keyword in charge_text for keyword in keywords):
                found_cat = category
                break
        charge_mix_counts[found_cat] += 1

    charge_mix = []
    for cat, _ in CATEGORY_RULES:
        count = charge_mix_counts.get(cat, 0)
        if count > 0: charge_mix.append((cat, f"{round((count / total_bookings) * 100)}%", count))
    other_count = charge_mix_counts.get("Other / Unknown", 0)
    if other_count > 0: charge_mix.append(("Other / Unknown", f"{round((other_count / total_bookings) * 100)}%", other_count))
    charge_mix.sort(key=lambda x: x[2], reverse=True)

    # 3. City Breakdown
    cities = [rec.get("city", "Unknown") for rec in records]
    city_counts = Counter(c for c in cities if c != "Unknown")
    top_cities_raw = city_counts.most_common(9)
    top_cities = [(city, f"{round((count / total_bookings) * 100)}%", count) for city, count in top_cities_raw]
    
    known_city_total = sum(c[2] for c in top_cities)
    unknown_count = total_bookings - known_city_total
    if unknown_count > 0: top_cities.append(("All Other Cities", f"{round((unknown_count / total_bookings) * 100)}%", unknown_count))

    # 4. Charge Bars for visualization
    charge_bars = []
    for cat, pct_str, count in charge_mix:
        label = {"Family Violence / Assault": "Fam. Violence", "Drugs / Possession": "Drugs / Poss.", "Evading / Resisting": "Evading", "Warrants / Court / Bond": "Warrants", "Other / Unknown": "Other"}.get(cat, cat)
        color = "#a09890" if cat == "Other / Unknown" else "#c8a45a"
        charge_bars.append((label, int(pct_str.replace("%", "")), color))

    print("Successfully analyzed statistics.")
    return {
        "total_bookings": total_bookings,
        "top_charge": top_charge,
        "charge_mix": charge_mix,
        "cities": top_cities,
        "charge_bars": charge_bars,
    }

# ---------------------------------------------------------------------------
# HTML & PDF Generation
# ---------------------------------------------------------------------------

def render_html(data: dict) -> str:
    """Renders the HTML report by populating a template file."""
    print("Rendering HTML report...")
    try:
        with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
            template = f.read()
    except FileNotFoundError:
        print(f"FATAL: HTML template not found at {HTML_TEMPLATE_PATH}")
        raise

    # --- Helper functions for building HTML snippets ---
    def build_charge_mix_bars(items):
        """Build bar graph rows for Charge Mix section."""
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            # Use gray color for "Other / Unknown", amber/gold for all others
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
        """Build bar graph rows for Arrests by City section."""
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            # Use gray color for "All Other Cities", amber/gold for specific cities
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
        """Build bar graph rows for Charge Distribution section."""
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

    # --- Replace placeholders ---
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
    print(f"HTML report saved to {HTML_OUTPUT_PATH}")
    return template

async def generate_pdf_from_html(html_content: str):
    """Converts HTML content to a PDF file using Pyppeteer/Chromium."""
    print("Generating PDF report from HTML...")
    try:
        browser = await launch(executablePath="/usr/bin/chromium-browser", args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"])
        page = await browser.newPage()
        await page.setContent(html_content, {"waitUntil": "networkidle0"})
        await page.pdf({
            "path": PDF_OUTPUT_PATH,
            "format": "Letter",
            "printBackground": True,
            "margin": {"top": "0.5in", "right": "0.5in", "bottom": "0.5in", "left": "0.5in"},
        })
        await browser.close()
        print(f"PDF report saved to {PDF_OUTPUT_PATH}")
    except Exception as e:
        print(f"ERROR: Failed to generate PDF. {e}")
        # Don't raise, as we still want to try sending the HTML email

# ---------------------------------------------------------------------------
# Email Sending
# ---------------------------------------------------------------------------

def send_email(subject: str, html_body: str):
    """Sends an email with HTML body and a PDF attachment (if it exists)."""
    if not all([TO_EMAIL, SMTP_USER, SMTP_PASS]):
        print("WARNING: Missing one or more required email environment variables (TO_EMAIL, SMTP_USER, SMTP_PASS). Skipping email.")
        return

    print(f"Preparing to send email to {TO_EMAIL}...")
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if os.path.exists(PDF_OUTPUT_PATH):
        with open(PDF_OUTPUT_PATH, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
            pdf_attachment.add_header("Content-Disposition", f"attachment; filename={os.path.basename(PDF_OUTPUT_PATH)}")
            msg.attach(pdf_attachment)
            print("Attached PDF to email.")
    else:
        print("WARNING: PDF file not found. Sending email without attachment.")

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"FATAL: Failed to send email. Error: {e}")
        # Don't raise, just log the error.

# ---------------------------------------------------------------------------
# Kit (ConvertKit) Broadcast Integration (NEW)
# ---------------------------------------------------------------------------

def send_kit_broadcast(subject: str, html_body: str):
    """
    Sends the report as a Kit (ConvertKit) broadcast to subscribers.
    Strips the DOCTYPE, html, head, and body wrapper tags because Kit adds its own wrapper.
    """
    if not KIT_API_KEY:
        print("WARNING: Missing KIT_API_KEY environment variable. Skipping Kit broadcast.")
        return
    
    print("Preparing to send Kit broadcast...")
    
    # Strip the wrapper tags and extract only the body content
    # Find the opening <body> tag and closing </body> tag
    body_start = html_body.find("<body")
    if body_start == -1:
        print("WARNING: Could not find <body> tag in HTML. Sending full HTML to Kit.")
        kit_html = html_body
    else:
        # Find the end of the opening <body> tag
        body_tag_end = html_body.find(">", body_start)
        if body_tag_end == -1:
            print("WARNING: Malformed <body> tag. Sending full HTML to Kit.")
            kit_html = html_body
        else:
            # Find the closing </body> tag
            body_end = html_body.find("</body>", body_tag_end)
            if body_end == -1:
                print("WARNING: Could not find </body> tag. Sending content after <body> tag to Kit.")
                kit_html = html_body[body_tag_end + 1:]
            else:
                # Extract only the content between <body> and </body>
                kit_html = html_body[body_tag_end + 1:body_end]
    
    # Prepare the Kit API request
    api_url = "https://api.convertkit.com/v3/broadcasts"
    payload = {
        "api_key": KIT_API_KEY,
        "subject": subject,
        "content": kit_html.strip(),
        "description": f"Automated jail report broadcast for {subject}",
        "public": False,
        "published_at": None,  # Send immediately
        "send_at": None,  # Send immediately
        "thumbnail_url": "",
        "email_layout_template": "",
    }
    
    # Add api_secret if available for additional authentication
    if KIT_API_SECRET:
        payload["api_secret"] = KIT_API_SECRET
    
    try:
        response = requests.post(api_url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
        broadcast_id = result.get("broadcast", {}).get("id")
        print(f"Kit broadcast created successfully. Broadcast ID: {broadcast_id}")
    except requests.RequestException as e:
        print(f"ERROR: Failed to send Kit broadcast. Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")

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

    # 2. Apply name parsing fix (NEW)
    records = fix_embedded_booking_numbers(records)

    # 3. Analyze statistics
    stats = analyze_stats(records)

    # 4. Prepare data for the template
    report_date_str = report_dt.strftime("%-m/%-d/%Y")
    template_data = {
        **stats,
        "report_date": report_date_str,
        "arrests_date": (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y"),
        "report_date_display": report_dt.strftime("%A, %B %-d, %Y"),
        "bookings": sorted(records, key=lambda x: x.get("name", "")),
    }

    # 5. Generate HTML report
    html_content = render_html(template_data)

    # 6. Generate PDF report
    await generate_pdf_from_html(html_content)

    # 7. Send the email
    subject = f"Tarrant County Jail Report — {report_date_str}"
    send_email(subject, html_content)

    # 8. Send Kit broadcast (NEW)
    send_kit_broadcast(subject, html_content)

    print("--- Report generation process complete. ---")

if __name__ == "__main__":
    asyncio.run(main())
