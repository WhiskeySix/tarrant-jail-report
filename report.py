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
# NOTE: LOGIC IS LOCKED. Styling-only changes below.

PURPLE = "#b48cff"   # CLI purple accent
BG = "#0f1216"       # dark, not jet black
CARD = "#141922"
CARD_2 = "#11151c"
BORDER = "#273140"
TEXT = "#e6e9ee"
MUTED = "#aeb7c2"
MUTED_2 = "#8d97a3"
TABLE_BG = "#10151c"
TABLE_HEAD = "#141b24"

# Fonts:
# - Headings/subheadings: mono (intel/CLI)
# - Table body: readable UI font (easier on eyes)
FONT_MONO = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"
FONT_BODY = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

DEFAULT_BOOKED_BASE_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
DEFAULT_BOOKED_DAYS = 1
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
# PDF Parsing (Booked-In) — LOGIC LOCKED
# -----------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# These appear as header/boilerplate in the PDF and MUST NEVER leak into description.
JUNK_LINE_RE = re.compile(
    r"(Inmates Booked In During the Past 24 Hours|Report Date:|Page:\s*\d+\s*of\s*\d+|Inmate Name|Identifier|CID|Book In Date|Booking No\.|Description)",
    re.IGNORECASE,
)

# Detect a "CITY TX 761xx" tail
CITY_STATE_ZIP_RE = re.compile(r"\b([A-Z][A-Z ]+)\s+TX\s+(\d{5})\b", re.IGNORECASE)
ZIP_RE = re.compile(r"\b\d{5}\b")


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None  # holds (cid, date) when we see CID DATE line before NAME
    current = None  # current record dict

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
                # Pattern A: NAME CID DATE
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

                # Pattern B: CID DATE (line 1) + NAME (line 2)
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

                # If we hit a new line unexpectedly while pending exists, drop pending (prevents bleed)
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
    LOGIC LOCKED.
    - Booking numbers anchor charge chunks
    - Address lines collected before first charge anchor
    - Prevent report boilerplate/header bleed into description
    """
    if not ln or JUNK_LINE_RE.search(ln):
        return

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        pre = ln[: bookings[0].start()].strip()
        if pre and not JUNK_LINE_RE.search(pre):
            rec["addr_lines"].append(pre)

        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            chunk = clean_charge_text(chunk)
            if chunk:
                rec["charges"].append(chunk)
        return

    # No booking number found.
    if not rec.get("charges"):
        # treat as address fragment unless it looks like boilerplate
        if not JUNK_LINE_RE.search(ln):
            rec["addr_lines"].append(ln)
        return

    # Continuation of last charge (wrap)
    tail = clean_charge_text(ln)
    if tail:
        rec["charges"][-1] = (rec["charges"][-1] + " " + tail).strip()


def clean_charge_text(s: str) -> str:
    """
    LOGIC LOCKED: ensure Description contains charge-only text.
    Strip out:
    - Report boilerplate
    - Trailing CITY TX ZIP
    - Lone ZIP codes that sometimes drift into the charge column
    """
    if not s:
        return ""
    if JUNK_LINE_RE.search(s):
        return ""

    s2 = re.sub(r"\s+", " ", s).strip()

    # Remove any trailing "CITY TX 761xx"
    s2 = re.sub(r"\b[A-Z][A-Z ]+\s+TX\s+\d{5}\b", "", s2, flags=re.IGNORECASE).strip()

    # Remove any remaining lone ZIP tokens
    s2 = re.sub(r"\b\d{5}\b", "", s2).strip()

    # If it becomes empty after stripping, return ""
    return s2


def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    """
    LOGIC LOCKED: City-under-name extraction.
    Priority:
    1) Any line containing "CITY TX ZIP"
    2) Any line containing "CITY TX"
    3) Fallback: last token-y word line (best effort)
    """
    if not addr_lines:
        return ""

    # Normalize whitespace
    lines = [re.sub(r"\s+", " ", x).strip() for x in addr_lines if x and x.strip()]
    if not lines:
        return ""

    # Look for explicit "City TX 761xx"
    for ln in lines:
        m = CITY_STATE_ZIP_RE.search(ln)
        if m:
            return to_title_case_city(m.group(1))

    # Look for "City TX" without zip
    for ln in lines:
        m = re.search(r"\b([A-Z][A-Z ]+)\s+TX\b", ln, flags=re.IGNORECASE)
        if m:
            return to_title_case_city(m.group(1))

    # Fallback: last wordy fragment (avoid street numbers)
    for ln in reversed(lines):
        if re.search(r"\d", ln):
            continue
        if len(ln) >= 3:
            return to_title_case_city(ln)

    return ""


def to_title_case_city(s: str) -> str:
    s = re.sub(r"\s+", " ", (s or "")).strip()
    if not s:
        return ""
    # "HALTOM CITY" -> "Haltom City"
    parts = s.split(" ")
    out = []
    for p in parts:
        if not p:
            continue
        if p.upper() in {"TX", "USA"}:
            continue
        out.append(p[0].upper() + p[1:].lower() if len(p) > 1 else p.upper())
    return " ".join(out).strip()


def finalize_record(rec: dict) -> dict:
    charges = []
    for c in rec.get("charges", []):
        c2 = re.sub(r"\s+", " ", c).strip()
        c2 = clean_charge_text(c2)
        if c2:
            charges.append(c2)

    addr_lines = []
    for a in rec.get("addr_lines", []):
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2 and not JUNK_LINE_RE.search(a2):
            addr_lines.append(a2)

    city = extract_city_from_addr_lines(addr_lines)

    return {
        "name": rec["name"],
        "book_in_date": rec["book_in_date"],
        "city": city,
        "description": "\n".join(charges),
    }


# -----------------------------
# HTML rendering — STYLING ONLY (logic unchanged)
# -----------------------------

def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def compute_most_common_charge(records: list[dict]) -> str:
    """
    LOGIC already present previously (display-only). Uses the first charge line per record.
    """
    c = Counter()
    for r in records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first = desc.splitlines()[0].strip()
        if first:
            c[first] += 1
    if not c:
        return "Unknown"
    return c.most_common(1)[0][0]


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    most_common = compute_most_common_charge(booked_records)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r["name"])
        city = html_escape(r.get("city", "")).strip()
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")
        date = html_escape(r["book_in_date"])

        name_block = f"""
          <div style="font-family:{FONT_MONO}; font-weight:900; color:{PURPLE}; letter-spacing:0.3px; font-size:14px;">
            {name}
          </div>
          <div style="margin-top:6px; font-family:{FONT_BODY}; color:{MUTED_2}; font-size:13px; line-height:1.2;">
            {city if city else "&nbsp;"}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; font-family:{FONT_BODY};">{name_block}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; white-space:nowrap; font-family:{FONT_BODY};">{date}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; font-family:{FONT_BODY};">{desc}</td>
          </tr>
        """)

    # Intel header blocks (existing layout, styling only)
    intel_header = f"""
    <div style="display:flex; flex-wrap:wrap; gap:10px; margin-top:16px;">
      <div style="padding:10px 14px; border-radius:999px; background:#e6edf7; color:#0f1216; font-family:{FONT_MONO}; font-weight:900; letter-spacing:0.5px; font-size:13px;">
        UNCLASSIFIED // FOR INFORMATIONAL USE ONLY
      </div>
      <div style="padding:10px 14px; border-radius:999px; border:1px solid {BORDER}; background:{CARD_2}; color:{TEXT}; font-family:{FONT_MONO}; font-weight:800; letter-spacing:0.4px; font-size:13px;">
        SOURCE: TARRANT COUNTY (CJ REPORTS)
      </div>
    </div>

    <div style="margin-top:14px; display:grid; grid-template-columns: 1fr; gap:10px;">
      <div style="border:1px solid {BORDER}; border-radius:14px; background:{CARD_2}; padding:14px 16px;">
        <div style="font-family:{FONT_MONO}; color:{MUTED}; letter-spacing:2px; font-size:12px;">REPORT DATE</div>
        <div style="margin-top:6px; font-family:{FONT_MONO}; color:{TEXT}; font-weight:900; font-size:26px;">{header_date_str}</div>
      </div>
      <div style="border:1px solid {BORDER}; border-radius:14px; background:{CARD_2}; padding:14px 16px;">
        <div style="font-family:{FONT_MONO}; color:{MUTED}; letter-spacing:2px; font-size:12px;">ARRESTS DATE</div>
        <div style="margin-top:6px; font-family:{FONT_MONO}; color:{TEXT}; font-weight:900; font-size:26px;">{arrests_date}</div>
      </div>
      <div style="border:1px solid {BORDER}; border-radius:14px; background:{CARD_2}; padding:14px 16px;">
        <div style="font-family:{FONT_MONO}; color:{MUTED}; letter-spacing:2px; font-size:12px;">RECORDS</div>
        <div style="margin-top:6px; font-family:{FONT_MONO}; color:{TEXT}; font-weight:900; font-size:26px;">{total}</div>
      </div>
    </div>
    """

    summary_block = f"""
      <div style="margin-top:18px; border:1px solid {BORDER}; border-radius:14px; background:{CARD_2}; padding:14px 16px;">
        <div style="font-family:{FONT_MONO}; font-size:14px; color:{MUTED}; line-height:1.6;">
          Total bookings in the last 24 hours:
          <span style="color:{PURPLE}; font-weight:900;">{total}</span>
          <br/>
          Most common charge:
          <span style="color:{PURPLE}; font-weight:900;">{html_escape(most_common)}</span>
        </div>
      </div>
    """

    source_line = f"""
      <div style="margin-top:16px; color:{MUTED}; font-size:14px; line-height:1.6; font-family:{FONT_MONO};">
        This report is automated from Tarrant County data.
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
<body style="margin:0; padding:0; background:{BG}; color:{TEXT}; font-family:{FONT_BODY};">
  <div style="max-width:900px; margin:0 auto; padding:22px 16px 40px;">
    <div style="background:{CARD}; border:1px solid {BORDER}; border-radius:18px; padding:22px 22px 18px;">

      <div style="font-family:{FONT_MONO}; font-size:46px; font-weight:1000; letter-spacing:-0.8px; line-height:1.05; color:{TEXT};">
        Tarrant County Jail Report — {header_date_str}
      </div>

      {intel_header}

      {source_line}

      <div style="margin-top:22px; font-family:{FONT_MONO}; font-size:40px; font-weight:1000; letter-spacing:-0.4px; color:{TEXT};">
        Booked-In (Last 24 Hours)
      </div>

      {summary_block}

      <div style="margin-top:14px; color:{MUTED}; font-size:14px; font-family:{FONT_MONO};">
        Showing first {shown} of {total} records.
      </div>

      <div style="margin-top:14px; overflow:hidden; border-radius:14px; border:1px solid {BORDER}; background:{TABLE_BG};">
        <table style="width:100%; border-collapse:collapse;">
          <thead>
            <tr style="background:{TABLE_HEAD};">
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:800; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO};">Name</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:800; border-bottom:1px solid {BORDER}; width:130px; font-family:{FONT_MONO};">Book In Date</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:800; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO};">Description</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html)}
          </tbody>
        </table>
      </div>

    </div>
  </div>
</body>
</html>
""".strip()


# -----------------------------
# Email sending — LOGIC LOCKED
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
# Main — LOGIC LOCKED
# -----------------------------

def main():
    booked_base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    booked_day = env("BOOKED_DAY", "01").strip()

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
