import os
import re
import ssl
import smtplib
import requests
from io import BytesIO
from datetime import datetime, timedelta
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pdfplumber


# -----------------------------
# Config / helpers
# -----------------------------

# STYLE ONLY (per your request)
AROUND_TABLE_BG = "#5E807F"   # area around the table
TABLE_BG = "#082D0F"          # table background (assumed missing last digit in your note)
TABLE_TEXT = "#24C783"        # table font color

# Keep the "intel / CLI" vibe for headers/subheaders
MONO = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"

# Use an easier-to-read font for table content (but keep the CLI color)
SANS = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

# Slightly darker frame tones (still consistent with your “not jet black” request)
PAGE_BG = AROUND_TABLE_BG
CARD_BG = "#4f6f6e"     # slightly darker than #5E807F so cards pop
BORDER = "#355352"
TEXT = "#0b1412"        # dark text used OUTSIDE table on light-ish background
MUTED = "#1a2b2a"

ACCENT = TABLE_TEXT     # keep accents consistent with table font color

DEFAULT_BOOKED_BASE_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "250"))


def env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v if v is not None else default


def safe_int(v: str, default: int) -> int:
    try:
        v = (v or "").strip()
        if not v:
            return default
        return int(v)
    except Exception:
        return default


def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# -----------------------------
# PDF Parsing (Booked-In)
# -----------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# Common “header bleed” lines that should never become charges/city
JUNK_LINE_RE = re.compile(
    r"(Inmates\s+Booked\s+In\s+During\s+the\s+Past\s+24\s+Hours|"
    r"Inmate\s+Name\s+Identifier|"
    r"Book\s+In\s+Date|"
    r"Booking\s+No\.\s*Description|"
    r"Report\s+Date:|"
    r"Page:\s*\d+\s+of\s+\d+)",
    re.IGNORECASE,
)


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def ensure_rec_keys(rec: dict) -> None:
    if "addr_lines" not in rec or not isinstance(rec.get("addr_lines"), list):
        rec["addr_lines"] = []
    if "charges" not in rec or not isinstance(rec.get("charges"), list):
        rec["charges"] = []


def clean_charge_text(s: str) -> str:
    """
    Keep only the charge text (strip location tails like 'FORT WORTH TX 76107'
    and strip obvious street-address fragments if they slipped in).
    """
    s = re.sub(r"\s+", " ", (s or "")).strip()

    # Remove trailing "CITY TX 761xx" (common pattern)
    s = re.sub(r"\s+[A-Z][A-Z ]+\s+TX\s+\d{5}\s*$", "", s).strip()

    # Remove trailing "TX 761xx" if city missing
    s = re.sub(r"\s+TX\s+\d{5}\s*$", "", s).strip()

    # Remove trailing street address blocks if present (best-effort, conservative)
    s = re.sub(
        r"\s+\d{1,6}\s+[A-Z0-9][A-Z0-9' \-]+(?:\s+[A-Z0-9][A-Z0-9' \-]+){0,5}\s+"
        r"(AVE|AV|ST|RD|DR|LN|CT|BLVD|PKWY|HWY|WAY|TRL|CIR|PL|TER)\b.*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()

    return s


def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    """
    City is displayed under the name.
    We pull the best candidate from address lines WITHOUT changing parsing logic.
    """
    if not addr_lines:
        return ""

    # Prefer lines that look like "CITY TX 76xxx"
    for ln in reversed(addr_lines):
        u = re.sub(r"\s+", " ", ln.strip()).upper()
        m = re.search(r"\b([A-Z][A-Z ]+)\s+TX\b", u)
        if m:
            city = m.group(1).strip().title()
            return city

    # Otherwise: last non-empty line that isn't junk and doesn't look like a street address
    for ln in reversed(addr_lines):
        t = re.sub(r"\s+", " ", ln.strip())
        if not t:
            continue
        if JUNK_LINE_RE.search(t):
            continue
        # if it has digits, it's probably a street line; skip
        if re.search(r"\d", t):
            continue
        return t.title()

    return ""


def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None  # holds (cid, date)
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
                # Drop known junk/header lines early
                if JUNK_LINE_RE.search(ln):
                    continue

                mA = NAME_CID_DATE_RE.match(ln)
                if mA:
                    if current:
                        records.append(finalize_record(current))
                    current = {
                        "name": mA.group("name").strip(),
                        "cid": mA.group("cid").strip(),
                        "book_in_date": mA.group("date").strip(),
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
                    pending = (mB.group("cid").strip(), mB.group("date").strip())
                    continue

                if pending and NAME_ONLY_RE.match(ln):
                    current = {
                        "name": ln.strip(),
                        "cid": pending[0],
                        "book_in_date": pending[1],
                        "addr_lines": [],
                        "charges": [],
                    }
                    pending = None
                    continue

                if pending and not current and ln:
                    pending = None

                if not current:
                    continue

                apply_content_line(current, ln)

        if current:
            records.append(finalize_record(current))

    return report_dt, records


def apply_content_line(rec: dict, ln: str) -> None:
    """
    Same parsing behavior; just defensive against missing keys.
    Booking numbers anchor charges.
    """
    ensure_rec_keys(rec)

    # Drop junk lines that sometimes appear mid-stream
    if JUNK_LINE_RE.search(ln):
        return

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        pre = ln[: bookings[0].start()].strip()
        if pre:
            rec["addr_lines"].append(pre)

        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            if chunk:
                rec["charges"].append(chunk)
        return

    if not rec["charges"]:
        rec["addr_lines"].append(ln)
        return

    # continuation line
    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    ensure_rec_keys(rec)

    addr_lines = []
    for a in rec["addr_lines"]:
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2 and not JUNK_LINE_RE.search(a2):
            addr_lines.append(a2)

    charges_clean = []
    for c in rec["charges"]:
        c2 = clean_charge_text(c)
        if c2:
            charges_clean.append(c2)

    city = extract_city_from_addr_lines(addr_lines)

    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": city,
        "description": "\n".join(charges_clean),
    }


# -----------------------------
# HTML rendering
# -----------------------------

def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def most_common_charge(booked_records: list[dict]) -> str:
    """
    Uses the first charge line for each record as the “primary” charge.
    (No parsing logic changes; just a display stat.)
    """
    primaries = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first = desc.splitlines()[0].strip()
        if first:
            primaries.append(first)

    if not primaries:
        return "Unknown"

    c = Counter(primaries)
    return c.most_common(1)[0][0]


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)
    common = most_common_charge(booked_records)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape(r.get("city", ""))
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")
        date = html_escape(r.get("book_in_date", ""))

        name_block = f"""
          <div style="font-family:{MONO}; font-weight:900; color:{TABLE_TEXT}; letter-spacing:0.2px;">
            {name}
          </div>
          <div style="margin-top:6px; font-family:{MONO}; color:{TABLE_TEXT}; font-size:13px; line-height:1.35; opacity:0.95;">
            {city if city else "&nbsp;"}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:14px 14px; border-top:1px solid {BORDER}; vertical-align:top; color:{TABLE_TEXT}; font-family:{SANS};">
              {name_block}
            </td>
            <td style="padding:14px 14px; border-top:1px solid {BORDER}; vertical-align:top; color:{TABLE_TEXT}; white-space:nowrap; font-family:{SANS};">
              {date}
            </td>
            <td style="padding:14px 14px; border-top:1px solid {BORDER}; vertical-align:top; color:{TABLE_TEXT}; font-family:{SANS};">
              {desc}
            </td>
          </tr>
        """)

    # Intel header blocks
    pills = f"""
      <div style="display:flex; gap:12px; flex-wrap:wrap; margin-top:14px;">
        <div style="background:#111; color:#f3f3f3; border-radius:999px; padding:10px 14px; font-family:{MONO}; font-weight:900; letter-spacing:0.6px;">
          UNCLASSIFIED // FOR INFORMATIONAL USE ONLY
        </div>
        <div style="background:#e7eceb; color:#111; border-radius:999px; padding:10px 14px; font-family:{MONO}; font-weight:900; letter-spacing:0.6px; border:1px solid #c8d0ce;">
          SOURCE: TARRANT COUNTY (CJ REPORTS)
        </div>
      </div>
    """

    meta_cards = f"""
      <div style="display:grid; grid-template-columns: 1fr; gap:12px; margin-top:16px;">
        <div style="background:#e7eceb; border:1px solid #c8d0ce; border-radius:14px; padding:14px 16px;">
          <div style="font-family:{MONO}; color:#334; opacity:0.85; letter-spacing:2px; font-size:12px;">REPORT DATE</div>
          <div style="font-family:{MONO}; font-weight:900; font-size:30px; color:#111;">{header_date_str}</div>
        </div>
        <div style="background:#e7eceb; border:1px solid #c8d0ce; border-radius:14px; padding:14px 16px;">
          <div style="font-family:{MONO}; color:#334; opacity:0.85; letter-spacing:2px; font-size:12px;">ARRESTS DATE</div>
          <div style="font-family:{MONO}; font-weight:900; font-size:30px; color:#111;">{arrests_date}</div>
        </div>
        <div style="background:#e7eceb; border:1px solid #c8d0ce; border-radius:14px; padding:14px 16px;">
          <div style="font-family:{MONO}; color:#334; opacity:0.85; letter-spacing:2px; font-size:12px;">RECORDS</div>
          <div style="font-family:{MONO}; font-weight:900; font-size:30px; color:#111;">{total}</div>
        </div>
      </div>
    """

    summary_box = f"""
      <div style="margin-top:18px; background:#e7eceb; border:1px solid #c8d0ce; border-radius:14px; padding:14px 16px;">
        <div style="font-family:{MONO}; font-size:16px; color:#111; opacity:0.9;">
          Total bookings in the last 24 hours:
          <span style="font-family:{MONO}; color:{ACCENT}; font-weight:900;">{total}</span>
        </div>
        <div style="margin-top:10px; font-family:{MONO}; font-size:16px; color:#111; opacity:0.9;">
          Most common charge:
          <span style="font-family:{MONO}; color:{ACCENT}; font-weight:900;">{html_escape(common)}</span>
        </div>
      </div>
    """

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Tarrant County Jail Report — {header_date_str}</title>
</head>

<body style="margin:0; padding:0; background:{PAGE_BG}; color:{TEXT}; font-family:{SANS};">
  <div style="max-width:900px; margin:0 auto; padding:26px 18px 40px;">

    <div style="background:{CARD_BG}; border:1px solid {BORDER}; border-radius:18px; padding:22px 22px 18px;">
      <div style="font-family:{MONO}; font-size:50px; font-weight:900; letter-spacing:-0.6px; line-height:1.05; color:#0b1412;">
        Tarrant County<br/>Jail Report —<br/>{header_date_str}
      </div>

      {pills}
      {meta_cards}

      <div style="margin-top:18px; height:1px; background:{BORDER};"></div>

      <div style="margin-top:18px; color:{MUTED}; font-family:{MONO}; font-size:18px; line-height:1.35;">
        This report is automated from Tarrant County data.
      </div>

      <div style="margin-top:26px; font-family:{MONO}; font-size:44px; font-weight:900; letter-spacing:-0.3px; color:#0b1412;">
        Booked-In (Last 24 Hours)
      </div>

      {summary_box}

      <div style="margin-top:18px; color:{MUTED}; font-family:{MONO}; font-size:16px;">
        Showing first {shown} of {total} records.
      </div>

      <!-- AREA AROUND TABLE -->
      <div style="margin-top:16px; background:{AROUND_TABLE_BG}; padding:12px; border-radius:16px; border:1px solid {BORDER};">

        <!-- TABLE -->
        <div style="overflow:hidden; border-radius:14px; border:1px solid {BORDER};">
          <table style="width:100%; border-collapse:collapse; background:{TABLE_BG};">
            <thead>
              <tr style="background:{TABLE_BG};">
                <th style="text-align:left; padding:12px 14px; color:{TABLE_TEXT}; font-family:{MONO}; font-weight:900; border-bottom:1px solid {BORDER}; opacity:0.95;">
                  Name
                </th>
                <th style="text-align:left; padding:12px 14px; color:{TABLE_TEXT}; font-family:{MONO}; font-weight:900; border-bottom:1px solid {BORDER}; width:120px; opacity:0.95;">
                  Book In Date
                </th>
                <th style="text-align:left; padding:12px 14px; color:{TABLE_TEXT}; font-family:{MONO}; font-weight:900; border-bottom:1px solid {BORDER}; opacity:0.95;">
                  Description
                </th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>
        </div>

      </div>
    </div>
  </div>
</body>
</html>
""".strip()


# -----------------------------
# Email sending
# -----------------------------

def send_email(subject: str, html_body: str) -> None:
    to_email = env("TO_EMAIL", "").strip()
    smtp_user = env("SMTP_USER", "").strip()
    smtp_pass = env("SMTP_PASS", "").strip()

    smtp_host = env("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port = safe_int(env("SMTP_PORT", "465"), 465)

    if not to_email or not smtp_user or not smtp_pass:
        raise RuntimeError("Missing required email env vars: TO_EMAIL, SMTP_USER, SMTP_PASS")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_email], msg.as_string())


# -----------------------------
# Main
# -----------------------------

def main():
    booked_base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    booked_day = env("BOOKED_DAY", "01").strip()  # keep simple: day 01
    booked_url = f"{booked_base}/{booked_day}.PDF"

    pdf_bytes = fetch_pdf(booked_url)
    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
