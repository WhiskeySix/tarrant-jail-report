import os
import re
import ssl
import smtplib
import html as html_lib
import asyncio
import subprocess
from io import BytesIO
from datetime import datetime, timedelta
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

import pdfplumber
import requests
from pyppeteer import launch


# ----------------------------
# Config
# ----------------------------
BOOKED_BASE_URL = os.getenv(
    "BOOKED_BASE_URL",
    "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
)
BOOKED_DAY = os.getenv("BOOKED_DAY")  # Optional; if not set we auto-detect
TO_EMAIL = os.getenv("TO_EMAIL", "j.jameshurt@gmail.com")

SMTP_USER = os.getenv("SMTP_USER")      # recommended: your gmail address
SMTP_PASS = os.getenv("SMTP_PASS")      # gmail app password (NOT your normal password)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))

HTML_TEMPLATE_PATH = "daily_report_template.html"

OUT_DIR = "reports"  # committed folder
TMP_HTML = "daily_jail_report.html"
TMP_PDF = "daily_jail_report.pdf"


# ----------------------------
# Parsing patterns (battle-tested style)
# ----------------------------
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
    ("DWI / Alcohol", ["DWI", "INTOX", "BAC", "DUI", "ALCOHOL", "DRUNK", "INTOXICATED", "PUBLIC INTOX", "OPEN CONT"]),
    ("Drugs / Possession", ["POSS", "POSS CS", "CONTROLLED SUB", "CS", "DRUG", "NARC", "MARIJ", "METH", "COCAINE", "HEROIN", "PARAPH"]),
    ("Family Violence / Assault", ["FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "CHOKE", "STRANG", "DOMESTIC"]),
    ("Theft / Fraud", ["THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "IDENTITY", "STOLEN", "SHOPLIFT"]),
    ("Weapons", ["WEAPON", "FIREARM", "GUN", "UCW", "UNL CARRYING"]),
    ("Evading / Resisting", ["EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"]),
    ("Warrants / Court / Bond", ["WARRANT", "FTA", "FAIL TO APPEAR", "BOND", "PAROLE", "PROBATION"]),
]

EMBEDDED_BOOKING_RE = re.compile(r"(\d{2}-\d{7})")


# ----------------------------
# Utilities
# ----------------------------
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
    charges_clean = [clean_charge_line(c) for c in rec.get("charges", []) if c]
    # de-dupe while preserving order
    charges = []
    for c in charges_clean:
        if c and c not in charges:
            charges.append(c)

    addr_lines = [normalize_ws(a) for a in rec.get("addr_lines", []) if a and not is_junk_line(a)]
    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": extract_city_from_addr_lines(addr_lines),
        "description": ", ".join(charges),
    }


# ----------------------------
# Fetch PDF
# ----------------------------
def fetch_pdf(url: str) -> bytes:
    print(f"Fetching PDF from: {url}")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# ----------------------------
# Parse PDF
# ----------------------------
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
        name = rec.get("name", "")
        match = EMBEDDED_BOOKING_RE.search(name)
        if match:
            booking_start = match.start()
            clean_name = name[:booking_start].strip()
            extra = name[booking_start:].strip()
            existing_desc = rec.get("description", "")
            rec["name"] = clean_name
            rec["description"] = f"{extra}, {existing_desc}".strip(", ").strip()
        fixed.append(rec)
    return fixed


# ----------------------------
# Stats
# ----------------------------
def analyze_stats(records: list[dict]) -> dict:
    total = len(records)
    if total == 0:
        return {"total_bookings": 0, "top_charge": "N/A", "charge_mix": [], "cities": [], "charge_bars": []}

    # top charge = first fragment of description
    first_charges = [
        rec.get("description", "").split(",")[0].strip().upper()
        for rec in records
        if rec.get("description")
    ]
    top_charge = Counter(first_charges).most_common(1)[0][0] if first_charges else "N/A"

    # charge mix
    charge_mix_counts = Counter()
    for rec in records:
        txt = (rec.get("description") or "").upper()
        found = "Other / Unknown"
        for cat, keys in CATEGORY_RULES:
            if any(k in txt for k in keys):
                found = cat
                break
        charge_mix_counts[found] += 1

    charge_mix = []
    for cat, _keys in CATEGORY_RULES:
        count = charge_mix_counts.get(cat, 0)
        if count:
            charge_mix.append((cat, f"{round((count/total)*100)}%", count))
    other_count = charge_mix_counts.get("Other / Unknown", 0)
    if other_count:
        charge_mix.append(("Other / Unknown", f"{round((other_count/total)*100)}%", other_count))
    charge_mix.sort(key=lambda x: x[2], reverse=True)

    # cities
    cities_raw = [rec.get("city", "Unknown") for rec in records]
    city_counts = Counter(c for c in cities_raw if c != "Unknown")
    top9 = city_counts.most_common(9)
    cities = [(city, f"{round((count/total)*100)}%", count) for city, count in top9]
    known = sum(c[2] for c in cities)
    other = total - known
    if other:
        cities.append(("All Other Cities", f"{round((other/total)*100)}%", other))

    # charge distribution bars (same order as charge_mix)
    label_map = {
        "Family Violence / Assault": "Fam. Violence",
        "Drugs / Possession": "Drugs / Poss.",
        "Evading / Resisting": "Evading",
        "Warrants / Court / Bond": "Warrants",
        "Other / Unknown": "Other",
    }
    charge_bars = []
    for cat, pct_str, _count in charge_mix:
        pct = int(pct_str.replace("%", ""))
        label = label_map.get(cat, cat)
        color = "#a09890" if cat in ("Other / Unknown",) else "#c8a45a"
        charge_bars.append((label, pct, color))

    return {
        "total_bookings": total,
        "top_charge": top_charge.title(),
        "charge_mix": charge_mix,
        "cities": cities,
        "charge_bars": charge_bars,
    }


# ----------------------------
# HTML render
# ----------------------------
def render_html(data: dict) -> str:
    with open(HTML_TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()

    def build_bars(items, is_city=False):
        rows = []
        for label, pct_str, count in items:
            pct = int(str(pct_str).replace("%", ""))
            is_other = (label == "All Other Cities") or (label == "Other / Unknown")
            color = "#a09890" if is_other else "#c8a45a"
            label_style = "color:#999590; font-style:italic;" if (is_city and label == "All Other Cities") else "color:#666360;"
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

    def build_distribution(items):
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
        "{{report_date}}": data.get("report_date", ""),
        "{{report_date_display}}": data.get("report_date_display", ""),
        "{{arrests_date}}": data.get("arrests_date", ""),
        "{{total_bookings}}": str(data.get("total_bookings", 0)),
        "{{top_charge}}": html_lib.escape(data.get("top_charge", "N/A")),
        "{{charge_mix_rows}}": build_bars(data.get("charge_mix", []), is_city=False),
        "{{city_rows}}": build_bars(data.get("cities", []), is_city=True),
        "{{bar_rows}}": build_distribution(data.get("charge_bars", [])),
        "{{booking_rows}}": build_booking_rows(data.get("bookings", [])),
    }
    for k, v in replacements.items():
        template = template.replace(k, v)

    with open(TMP_HTML, "w", encoding="utf-8") as f:
        f.write(template)

    return template


# ----------------------------
# PDF render (same styling)
# ----------------------------
async def generate_pdf_from_html(html_content: str, pdf_path: str):
    # find chromium
    chromium = None
    for candidate in ["chromium", "chromium-browser", "google-chrome", "google-chrome-stable"]:
        p = subprocess.run(["bash", "-lc", f"command -v {candidate}"], capture_output=True, text=True)
        if p.returncode == 0:
            chromium = p.stdout.strip()
            break

    launch_kwargs = {"args": ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]}
    if chromium:
        launch_kwargs["executablePath"] = chromium

    browser = await launch(**launch_kwargs)
    page = await browser.newPage()
    await page.setViewport({"width": 1200, "height": 1600, "deviceScaleFactor": 2})
    await page.setContent(html_content, {"waitUntil": "networkidle0"})
    await page.pdf({
        "path": pdf_path,
        "format": "Letter",
        "printBackground": True,
        "margin": {"top": "0.5in", "right": "0.5in", "bottom": "0.5in", "left": "0.5in"},
    })
    await browser.close()


# ----------------------------
# Email send (HTML + PDF)
# ----------------------------
def send_email(subject: str, html_body: str, pdf_path: str):
    if not all([SMTP_USER, SMTP_PASS, TO_EMAIL]):
        raise RuntimeError("Missing SMTP_USER, SMTP_PASS, or TO_EMAIL env vars.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as f:
            att = MIMEApplication(f.read(), _subtype="pdf")
        att.add_header("Content-Disposition", "attachment", filename=os.path.basename(pdf_path))
        msg.attach(att)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())


# ----------------------------
# Helpers: determine which PDF to fetch
# ----------------------------
def detect_day_for_today() -> str:
    # Tarrant PDF folder often uses day-of-month filenames like 01.PDF, 02.PDF, etc.
    # We'll try today's day first; if fail, fallback yesterday.
    today = datetime.now()
    candidates = [today.day, (today - timedelta(days=1)).day]
    for d in candidates:
        day_str = f"{d:02d}"
        url = f"{BOOKED_BASE_URL.rstrip('/')}/{day_str}.PDF"
        try:
            r = requests.head(url, timeout=20)
            if r.status_code == 200:
                return day_str
        except Exception:
            pass
    # default
    return f"{today.day:02d}"


# ----------------------------
# Main
# ----------------------------
async def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    day = BOOKED_DAY or detect_day_for_today()
    pdf_url = f"{BOOKED_BASE_URL.rstrip('/')}/{day}.PDF"

    pdf_bytes = fetch_pdf(pdf_url)
    report_dt, records = parse_booked_in(pdf_bytes)
    records = fix_embedded_booking_numbers(records)

    stats = analyze_stats(records)

    # Note: report_dt from PDF is "report date"
    report_date_str = report_dt.strftime("%-m/%-d/%Y") if hasattr(report_dt, "strftime") else datetime.now().strftime("%-m/%-d/%Y")
    arrests_date_str = (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y")

    template_data = {
        **stats,
        "report_date": report_date_str,
        "arrests_date": arrests_date_str,
        "report_date_display": report_dt.strftime("%A, %B %-d, %Y"),
        "bookings": sorted(records, key=lambda x: x.get("name", "")),
    }

    html_content = render_html(template_data)

    # outputs committed to repo
    out_slug = report_dt.strftime("%Y-%m-%d")
    out_html = os.path.join(OUT_DIR, f"{out_slug}.html")
    out_pdf = os.path.join(OUT_DIR, f"{out_slug}.pdf")

    # write the final HTML file
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html_content)

    # generate PDF from same HTML (pixel match)
    await generate_pdf_from_html(html_content, out_pdf)

    # email it
    subject = f"Tarrant County Jail Report — {report_date_str}"
    send_email(subject, html_content, out_pdf)

    print(f"Done. Saved: {out_html} and {out_pdf}, emailed to {TO_EMAIL}.")


if __name__ == "__main__":
    asyncio.run(main())
