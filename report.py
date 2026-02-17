"""
Tarrant County Daily Jail Report (DailyJailReports.com)

- Fetch booked-in PDF
- Parse booking records (name/date/city/charges)
- Compute stats
- Render HTML using daily_report_template.html (placeholders {{...}})
- Generate PDF from HTML (Chromium headless)
- Write outputs to reports/
- Email HTML + attach PDF
- (No Kit / ConvertKit in this version)
"""

import os
import re
import ssl
import smtplib
import asyncio
import html as html_lib
from io import BytesIO
from datetime import datetime, timedelta
from collections import Counter
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.header import Header

import pdfplumber
import requests
from pyppeteer import launch


# =============================================================================
# CONFIG (Environment Variables)
# =============================================================================

BOOKED_BASE_URL = os.getenv(
    "BOOKED_BASE_URL",
    "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
).rstrip("/")

# If you ever need a specific day file like "01.PDF" etc:
BOOKED_DAY = os.getenv("BOOKED_DAY", "01").strip()  # not used by default fetch logic

TO_EMAIL = os.getenv("TO_EMAIL", "").strip()  # required
SMTP_USER = os.getenv("SMTP_USER", "").strip()  # required
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()  # required
SMTP_HOST = (os.getenv("SMTP_HOST") or "smtp.gmail.com").strip()

# ✅ Option C: robust even if SMTP_PORT is missing/blank/whitespace
SMTP_PORT = int((os.getenv("SMTP_PORT") or "465").strip())

# Chromium path (actions step will set this)
CHROME_PATH = (os.getenv("CHROME_PATH") or "").strip()

# Template + output locations
HTML_TEMPLATE_PATH = Path("daily_report_template.html")
REPORTS_DIR = Path("reports")
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


# =============================================================================
# Parsing Patterns (kept close to your baseline)
# =============================================================================

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

EMBEDDED_BOOKING_RE = re.compile(r"(\d{2}-\d{7})")

CATEGORY_RULES = [
    ("DWI / Alcohol", ["DWI", "INTOX", "BAC", "DUI", "ALCOHOL", "DRUNK", "INTOXICATED", "PUBLIC INTOX", "OPEN CONT"]),
    ("Drugs / Possession", ["POSS", "POSS CS", "CONTROLLED SUB", "CS", "DRUG", "NARC", "MARIJ", "METH", "COCAINE", "HEROIN", "PARAPH"]),
    ("Family Violence / Assault", ["FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "CHOKE", "STRANG", "DOMESTIC"]),
    ("Theft / Fraud", ["THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "IDENTITY", "STOLEN", "SHOPLIFT"]),
    ("Weapons", ["WEAPON", "FIREARM", "GUN", "UCW", "UNL CARRYING"]),
    ("Evading / Resisting", ["EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"]),
    ("Warrants / Court / Bond", ["WARRANT", "FTA", "FAIL TO APPEAR", "BOND", "PAROLE", "PROBATION"]),
]


# =============================================================================
# Fetch PDF
# =============================================================================

def fetch_pdf(url: str) -> bytes:
    print(f"Fetching PDF: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# =============================================================================
# Helpers
# =============================================================================

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
    charges_raw = [clean_charge_line(c) for c in rec.get("charges", []) if c]
    charges = []
    for c in charges_raw:
        if c and c not in charges:
            charges.append(c)
    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]
    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": extract_city_from_addr_lines(addr_lines),
        "description": ", ".join(charges),
    }


# =============================================================================
# PDF Parse
# =============================================================================

def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        # report date from first page
        report_dt = datetime.now()
        try:
            first_page_text = pdf.pages[0].extract_text() or ""
            m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", first_page_text)
            if m:
                report_dt = datetime.strptime(m.group(1), "%m/%d/%Y")
        except Exception:
            pass

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

    print(f"Parsed records: {len(records)}")
    return report_dt, records


def fix_embedded_booking_numbers(records: list[dict]) -> list[dict]:
    fixed = []
    for rec in records:
        name = rec.get("name", "") or ""
        match = EMBEDDED_BOOKING_RE.search(name)
        if match:
            booking_start = match.start()
            clean_name = name[:booking_start].strip()
            extra_content = name[booking_start:].strip()
            existing_desc = rec.get("description", "") or ""
            rec["name"] = clean_name
            rec["description"] = f"{extra_content}, {existing_desc}".strip(", ").strip()
        fixed.append(rec)
    return fixed


# =============================================================================
# Stats
# =============================================================================

def analyze_stats(records: list[dict]) -> dict:
    total = len(records)
    if total == 0:
        return {"total_bookings": 0, "top_charge": "N/A", "charge_mix": [], "cities": [], "charge_bars": []}

    # top charge (first chunk)
    first_charges = []
    for r in records:
        desc = (r.get("description") or "").strip()
        if desc:
            first_charges.append(desc.split(",")[0].strip().upper())
    top_charge = Counter(first_charges).most_common(1)[0][0] if first_charges else "N/A"

    # charge mix
    charge_mix_counts = Counter()
    for r in records:
        txt = (r.get("description") or "").upper()
        found = "Other / Unknown"
        for cat, keys in CATEGORY_RULES:
            if any(k in txt for k in keys):
                found = cat
                break
        charge_mix_counts[found] += 1

    # keep category order + other last
    charge_mix = []
    for cat, _ in CATEGORY_RULES:
        c = charge_mix_counts.get(cat, 0)
        if c:
            charge_mix.append((cat, f"{round(c/total*100)}%", c))
    other_c = charge_mix_counts.get("Other / Unknown", 0)
    if other_c:
        charge_mix.append(("Other / Unknown", f"{round(other_c/total*100)}%", other_c))
    charge_mix.sort(key=lambda x: x[2], reverse=True)

    # cities (top 9 + all other)
    cities = [r.get("city", "Unknown") for r in records]
    city_counts = Counter(c for c in cities if c and c != "Unknown")
    top = city_counts.most_common(9)
    city_rows = [(c, f"{round(n/total*100)}%", n) for c, n in top]
    top_sum = sum(n for _, _, n in city_rows)
    other = total - top_sum
    if other > 0:
        city_rows.append(("All Other Cities", f"{round(other/total*100)}%", other))

    # distribution bars (same as charge_mix but condensed labels)
    label_map = {
        "Family Violence / Assault": "Fam. Violence",
        "Drugs / Possession": "Drugs / Poss.",
        "Evading / Resisting": "Evading",
        "Warrants / Court / Bond": "Warrants",
        "Other / Unknown": "Other",
    }
    bars = []
    for cat, pct_str, _count in charge_mix:
        pct = int(pct_str.replace("%", ""))
        label = label_map.get(cat, cat)
        color = "#a09890" if cat == "Other / Unknown" else "#c8a45a"
        bars.append((label, pct, color))

    return {
        "total_bookings": total,
        "top_charge": top_charge,
        "charge_mix": charge_mix,
        "cities": city_rows,
        "charge_bars": bars,
    }


# =============================================================================
# HTML Render
# =============================================================================

def render_html(template_data: dict) -> str:
    if not HTML_TEMPLATE_PATH.exists():
        raise FileNotFoundError(f"Missing template: {HTML_TEMPLATE_PATH}")

    template = HTML_TEMPLATE_PATH.read_text(encoding="utf-8")

    def build_charge_mix_rows(items):
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            color = "#a09890" if label == "Other / Unknown" else "#c8a45a"
            rows.append(f"""
<tr>
  <td style="padding:3px 0; width:140px; color:#666360; font-size:11px; vertical-align:middle;">{html_lib.escape(label)}</td>
  <td style="padding:3px 8px; vertical-align:middle;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
      <tr>
        <td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td>
        <td style="font-size:1px;">&nbsp;</td>
      </tr>
    </table>
  </td>
  <td style="padding:3px 0; width:70px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%&nbsp;<span style="color:#999590; font-weight:400; font-size:10px;">({count})</span></td>
</tr>
""".strip())
        return "\n".join(rows)

    def build_city_rows(items):
        rows = []
        for label, pct_str, count in items:
            pct = int(pct_str.replace("%", ""))
            color = "#a09890" if label == "All Other Cities" else "#c8a45a"
            label_style = "color:#999590; font-style:italic;" if label == "All Other Cities" else "color:#666360;"
            rows.append(f"""
<tr>
  <td style="padding:3px 0; width:140px; {label_style} font-size:11px; vertical-align:middle;">{html_lib.escape(label)}</td>
  <td style="padding:3px 8px; vertical-align:middle;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
      <tr>
        <td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td>
        <td style="font-size:1px;">&nbsp;</td>
      </tr>
    </table>
  </td>
  <td style="padding:3px 0; width:70px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%&nbsp;<span style="color:#999590; font-weight:400; font-size:10px;">({count})</span></td>
</tr>
""".strip())
        return "\n".join(rows)

    def build_bar_rows(items):
        rows = []
        for label, pct, color in items:
            rows.append(f"""
<tr>
  <td style="padding:3px 0; width:140px; color:#666360; font-size:11px; vertical-align:middle;">{html_lib.escape(label)}</td>
  <td style="padding:3px 8px; vertical-align:middle;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#e8e4dc; border-radius:2px;">
      <tr>
        <td style="width:{pct}%; background-color:{color}; height:14px; border-radius:2px; font-size:1px;">&nbsp;</td>
        <td style="font-size:1px;">&nbsp;</td>
      </tr>
    </table>
  </td>
  <td style="padding:3px 0; width:36px; color:#1a1a1a; font-weight:700; text-align:right; font-size:11px; vertical-align:middle;">{pct}%</td>
</tr>
""".strip())
        return "\n".join(rows)

    def build_booking_rows(items):
        rows = []
        for i, rec in enumerate(items, 1):
            bg = "#faf8f5" if i % 2 == 1 else "#f4f1eb"
            rows.append(f"""
<tr style="background-color:{bg};">
  <td style="padding:9px 12px; color:#999590; font-size:11px; border-bottom:1px solid #e8e4dc; vertical-align:top;">{i}</td>
  <td style="padding:9px 12px; color:#1a1a1a; font-weight:600; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html_lib.escape(rec.get("name",""))}</td>
  <td style="padding:9px 12px; color:#666360; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html_lib.escape(rec.get("book_in_date",""))}</td>
  <td style="padding:9px 12px; color:#444240; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:11px;">{html_lib.escape(rec.get("description",""))}</td>
  <td style="padding:9px 12px; color:#666360; border-bottom:1px solid #e8e4dc; vertical-align:top; font-size:12px;">{html_lib.escape(rec.get("city",""))}</td>
</tr>
""".strip())
        return "\n".join(rows)

    replacements = {
        "{{report_date}}": template_data["report_date"],
        "{{report_date_display}}": template_data["report_date_display"],
        "{{arrests_date}}": template_data["arrests_date"],
        "{{total_bookings}}": str(template_data["total_bookings"]),
        "{{top_charge}}": html_lib.escape(template_data["top_charge"]),
        "{{charge_mix_rows}}": build_charge_mix_rows(template_data["charge_mix"]),
        "{{city_rows}}": build_city_rows(template_data["cities"]),
        "{{bar_rows}}": build_bar_rows(template_data["charge_bars"]),
        "{{booking_rows}}": build_booking_rows(template_data["bookings"]),
    }

    for k, v in replacements.items():
        template = template.replace(k, v)

    return template


# =============================================================================
# PDF Generation (Chromium)
# =============================================================================

from pyppeteer import launch

async def generate_pdf_from_html(html_content: str):
    """Converts HTML content to a PDF file using Pyppeteer/Chromium."""
    print("Generating PDF report from HTML...")
    try:
        browser = await launch(
            headless=True,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
        )
        page = await browser.newPage()
        await page.setContent(html_content, {"waitUntil": "networkidle0"})

        await page.pdf({
            "path": PDF_OUTPUT_PATH,
            "format": "Letter",
            "printBackground": True,
            "margin": {"top": "0.5in", "right": "0.5in", "bottom": "0.5in", "left": "0.5in"},
        })

        await browser.close()
        print(f"PDF report saved to {PDF_OUTPUT_PATH} (exists={os.path.exists(PDF_OUTPUT_PATH)})")
    except Exception as e:
        print(f"ERROR: Failed to generate PDF. {e}")


# =============================================================================
# Email
# =============================================================================

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os, ssl, smtplib

def send_email(subject: str, html_body: str):
    """Sends an email with HTML body and a PDF attachment (if it exists)."""
    if not all([TO_EMAIL, SMTP_USER, SMTP_PASS]):
        print("WARNING: Missing TO_EMAIL/SMTP_USER/SMTP_PASS. Skipping email.")
        return

    print(f"Preparing to send email to {TO_EMAIL}...")

    # ✅ OUTER must be 'mixed' when attachments are included
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL

    # ✅ Nest HTML in an 'alternative' part
    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText("Your email client does not support HTML.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    msg.attach(alt)

    # Attach PDF (if created)
    if os.path.exists(PDF_OUTPUT_PATH):
        with open(PDF_OUTPUT_PATH, "rb") as f:
            pdf_attachment = MIMEApplication(f.read(), _subtype="pdf")
        pdf_attachment.add_header(
            "Content-Disposition",
            f'attachment; filename="{os.path.basename(PDF_OUTPUT_PATH)}"'
        )
        msg.attach(pdf_attachment)
        print(f"Attached PDF to email: {PDF_OUTPUT_PATH}")
    else:
        print(f"WARNING: PDF not found at {PDF_OUTPUT_PATH}. Email will be HTML-only.")

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASS)
            server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"FATAL: Failed to send email. Error: {e}")

# =============================================================================
# Main
# =============================================================================

async def main():
    print("--- Daily Jail Report start ---")

    # PDF URL: most deployments use base + "/01.PDF" style
    # Your earlier working setup used: f"{BOOKED_BASE_URL}/{BOOKED_DAY}.PDF"
    # We’ll keep that pattern for compatibility:
    pdf_url = f"{BOOKED_BASE_URL}/{BOOKED_DAY}.PDF"

    pdf_bytes = fetch_pdf(pdf_url)
    report_dt, records = parse_booked_in(pdf_bytes)
    records = fix_embedded_booking_numbers(records)

    stats = analyze_stats(records)

    # dates
    report_date_str = report_dt.strftime("%-m/%-d/%Y")
    arrests_date_str = (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y")
    display_str = report_dt.strftime("%A, %B %-d, %Y")

    # outputs
    stamp = report_dt.strftime("%Y-%m-%d")
    html_path = REPORTS_DIR / f"report_{stamp}.html"
    pdf_path = REPORTS_DIR / f"report_{stamp}.pdf"

    template_data = {
        **stats,
        "report_date": report_date_str,
        "arrests_date": arrests_date_str,
        "report_date_display": display_str,
        "bookings": sorted(records, key=lambda x: x.get("name", "")),
    }

    html_report = render_html(template_data)
    html_path.write_text(html_report, encoding="utf-8")
    print(f"Wrote HTML: {html_path}")

    # PDF
    try:
        await html_to_pdf(html_report, pdf_path)
        print(f"Wrote PDF: {pdf_path}")
    except Exception as e:
        print(f"PDF generation failed: {e}")
        pdf_path = None

    subject = f"Tarrant County Jail Report — {report_date_str}"
    send_email(subject, html_report, pdf_path)

    print("--- Done ---")


if __name__ == "__main__":
    asyncio.run(main())
