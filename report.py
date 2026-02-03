import os
import re
import ssl
import smtplib
import requests
from io import BytesIO
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pdfplumber


# -----------------------------
# Theme / constants
# -----------------------------

ORANGE = "#f4a261"  # orange used for totals + offender name
BG = "#111315"
CARD = "#1b1f23"
TEXT = "#d7d7d7"
MUTED = "#a8b0b7"
BORDER = "#2a2f34"

DEFAULT_BOOKED_BASE_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "250"))


# -----------------------------
# Env helpers
# -----------------------------

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


# -----------------------------
# Fetch PDF
# -----------------------------

def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# -----------------------------
# Parsing regex
# -----------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")

BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# City patterns we’ll extract
CITY_TX_ZIP_RE = re.compile(r"\b([A-Z][A-Z ]+?)\s+TX\s+(\d{5})\b")
CITY_TX_RE = re.compile(r"\b([A-Z][A-Z ]+?)\s+TX\b")
CITY_ONLY_RE = re.compile(r"^[A-Z][A-Z ]+$")

# Junk text that sometimes leaks into a row (PDF headers)
JUNK_SNIPPETS = [
    "Inmates Booked In During the Past 24 Hours",
    "Report Date:",
    "Page:",
    "Inmate Name",
    "Identifier",
    "CID",
    "Book In Date",
    "Booking No.",
    "Description",
]


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


# -----------------------------
# City + description cleanup
# -----------------------------

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_description_text(s: str) -> str:
    """
    Removes known header/junk phrases and collapses whitespace.
    """
    t = s or ""
    for j in JUNK_SNIPPETS:
        t = t.replace(j, " ")
    t = normalize_spaces(t)
    return t


def split_city_from_text(text: str) -> tuple[str, str]:
    """
    If text contains 'CITY TX 76102' or 'CITY TX', extract CITY and return:
      (city, text_without_city_segment)
    We only remove city segments when they appear as trailing location noise.
    """
    t = normalize_spaces(text)

    # Prefer CITY TX ZIP
    m = CITY_TX_ZIP_RE.search(t)
    if m:
        city = normalize_spaces(m.group(1))
        # Remove only the matched location chunk (leave charge wording intact)
        start, end = m.span()
        # If it appears at the end or near-end, strip from that point onward.
        if end >= len(t) - 1 or (len(t) - end) <= 2:
            return city, normalize_spaces(t[:start])
        # If it’s embedded, we still treat it as location noise and remove that substring.
        removed = normalize_spaces(t[:start] + " " + t[end:])
        return city, removed

    # CITY TX (no zip)
    m2 = CITY_TX_RE.search(t)
    if m2:
        city = normalize_spaces(m2.group(1))
        start, end = m2.span()
        if end >= len(t) - 1 or (len(t) - end) <= 2:
            return city, normalize_spaces(t[:start])
        removed = normalize_spaces(t[:start] + " " + t[end:])
        return city, removed

    return "", t


def extract_city_from_address_lines(addr_lines: list[str]) -> str:
    """
    Tries to pull CITY from address lines.
    We return CITY only (no TX, no zip, no street).
    """
    city = ""

    for raw in addr_lines or []:
        line = normalize_spaces(raw)

        # Match CITY TX ZIP
        m = CITY_TX_ZIP_RE.search(line)
        if m:
            city = normalize_spaces(m.group(1))
            continue

        # Match CITY TX
        m2 = CITY_TX_RE.search(line)
        if m2:
            city = normalize_spaces(m2.group(1))
            continue

        # Sometimes PDF gives a line that is just "FORT WORTH"
        if CITY_ONLY_RE.match(line) and len(line) >= 3 and len(line.split()) <= 4:
            city = line

    return city


# -----------------------------
# PDF parsing
# -----------------------------

def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None  # (cid, date) if we see CID DATE before NAME
    current = None  # current record

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
                # A) NAME CID DATE
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

                # B) CID DATE then NAME
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

                # If pending exists but we don’t see the expected NAME next, drop it.
                if pending and not current:
                    pending = None

                if not current:
                    continue

                apply_content_line(current, ln)

        if current:
            records.append(finalize_record(current))

    return report_dt, records


def apply_content_line(rec: dict, ln: str) -> None:
    """
    Splits lines into address fragments and charges.
    Booking numbers (e.g., 26-0259229) act like anchors between charge chunks.
    """
    # Skip obvious junk lines early
    if any(j in ln for j in JUNK_SNIPPETS):
        return

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        # before first booking: usually address
        pre = ln[: bookings[0].start()].strip()
        if pre:
            rec["addr_lines"].append(pre)

        # after bookings: charge chunks
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            if chunk:
                rec["charges"].append(chunk)
        return

    # no booking number:
    # if no charges yet, treat as address-ish
    if not rec["charges"]:
        rec["addr_lines"].append(ln)
        return

    # otherwise charge continuation (wrap)
    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    # Clean address lines
    addr_lines = []
    for a in rec.get("addr_lines", []):
        a2 = normalize_spaces(a)
        if a2:
            addr_lines.append(a2)

    # Clean charges (and strip junk phrases)
    charges = []
    for c in rec.get("charges", []):
        c2 = clean_description_text(c)
        if c2:
            charges.append(c2)

    # 1) City primarily from address lines
    city = extract_city_from_address_lines(addr_lines)

    # 2) If city is missing OR city leaked into description, extract city from description and strip it out
    cleaned_charges = []
    for ch in charges:
        c_city, stripped = split_city_from_text(ch)
        if not city and c_city:
            city = c_city
        # Ensure description = charge only
        stripped = clean_description_text(stripped)
        if stripped:
            cleaned_charges.append(stripped)

    # final description as multi-line (but charge-only)
    description = "\n".join([x for x in cleaned_charges if x])

    return {
        "name": rec.get("name", ""),
        "book_in_date": rec.get("book_in_date", ""),
        "city": (city or "").title() if city else "",  # nicer display
        "description": description,
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


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # arrests date is 1 day behind header date
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape(r.get("city", ""))
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")
        date = html_escape(r.get("book_in_date", ""))

        # Always render a city line; if missing, keep spacing consistent
        city_line = city if city else "&nbsp;"

        name_block = f"""
          <div style="font-weight:800; color:{ORANGE}; letter-spacing:0.2px;">{name}</div>
          <div style="margin-top:6px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{TEXT}; font-size:13px; line-height:1.35;">
            {city_line}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top;">{name_block}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; white-space:nowrap;">{date}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT};">{desc}</td>
          </tr>
        """)

    bookings_line = f"""
      <div style="margin-top:10px; font-size:15px; color:{MUTED};">
        Total bookings in the last 24 hours:
        <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{ORANGE}; font-weight:800;">
          {total}
        </span>
      </div>
    """

    source_line = f"""
      <div style="margin-top:18px; color:{MUTED}; font-size:14px; line-height:1.5;">
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
<body style="margin:0; padding:0; background:{BG}; color:{TEXT}; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <div style="max-width:900px; margin:0 auto; padding:26px 18px 40px;">
    <div style="background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:22px 22px 18px;">
      <div style="font-size:44px; font-weight:900; letter-spacing:-0.6px; line-height:1.05;">
        Tarrant County Jail Report — {header_date_str}
      </div>

      <div style="margin-top:10px; font-size:20px; color:{MUTED}; line-height:1.35;">
        Summary of arrests in Tarrant County for {arrests_date}
      </div>

      <div style="margin-top:18px; height:1px; background:{BORDER};"></div>

      {source_line}

      <div style="margin-top:26px; font-size:34px; font-weight:900; letter-spacing:-0.3px;">
        Booked-In (Last 24 Hours)
      </div>

      {bookings_line}

      <div style="margin-top:18px; color:{MUTED}; font-size:14px;">
        Showing first {shown} of {total} records.
      </div>

      <div style="margin-top:16px; overflow:hidden; border-radius:12px; border:1px solid {BORDER};">
        <table style="width:100%; border-collapse:collapse; background:#14181b;">
          <thead>
            <tr style="background:#1a1f23;">
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER};">Name</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER}; width:120px;">Book In Date</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER};">Description</th>
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
# Email sending
# -----------------------------

def send_email(subject: str, html_body: str) -> None:
    to_email = env("TO_EMAIL", "").strip()
    smtp_user = env("SMTP_USER", "").strip()
    smtp_pass = env("SMTP_PASS", "").strip()

    # safe defaults (NOT required vars)
    smtp_host = env("SMTP_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com"
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
    booked_day = env("BOOKED_DAY", "01").strip()  # keep simple/stable

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
