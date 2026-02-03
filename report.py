import os
import re
import ssl
import smtplib
import urllib.request
from io import BytesIO
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import Counter

import pdfplumber


# -----------------------------
# Config (safe defaults)
# -----------------------------
DEFAULT_BOOKED_PDF_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF"

MAX_ROWS = 250

# Orange you liked (used for name + booking count number)
ORANGE = "#F2A154"


# -----------------------------
# Helpers: environment
# -----------------------------
def env(name: str, default: str = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


# -----------------------------
# PDF download
# -----------------------------
def download_pdf(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; TarrantJailReport/1.0)"
        }
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


# -----------------------------
# Cleaning / parsing
# -----------------------------
_JUNK_PHRASES = [
    "INMATES BOOKED IN DURING THE PAST 24 HOURS",
    "INMATES BOOKED IN",
    "DURING THE PAST 24 HOURS",
    "REPORT DATE:",
    "PAGE:",
    "INMATE NAME",
    "NAME IDENTIFIER",
    "IDENTIFIER",
    "CID",
    "BOOK IN DATE",
    "BOOKING NO.",
    "DESCRIPTION",
]

# matches "CITY TX 76155" (city may be multiple words)
_CITY_STATE_ZIP_RE = re.compile(r"\b[A-Z][A-Z ]+\s+TX\s+\d{5}\b")

# matches street fragments like "2400 CYPRESS ST" or "#9201"
_STREET_ADDR_RE = re.compile(
    r"\b\d{1,6}\s+[A-Z0-9#'./-]+\s+(?:AVE|AV|ST|RD|DR|LN|PL|CT|BLVD|HWY|PKWY|WAY|TRL|CIR|TER|PK|LOOP|FWY)\b",
    re.IGNORECASE
)

_DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")


def clean_description(raw: str) -> str:
    """Keep only the charge text. Strip report junk + trailing city/zip + leaked addresses."""
    if not raw:
        return ""
    s = raw.strip()
    s = re.sub(r"\s+", " ", s)
    upper = s.upper()

    # If the report footer/header junk appears, cut description before it
    for phrase in _JUNK_PHRASES:
        idx = upper.find(phrase)
        if idx != -1:
            s = s[:idx].strip()
            upper = s.upper()
            break

    # Remove trailing "CITY TX ZIP" if it appears late in the string
    matches = list(_CITY_STATE_ZIP_RE.finditer(upper))
    if matches:
        last = matches[-1]
        if last.start() >= max(10, len(s) // 2):
            s = s[:last.start()].strip()

    # Remove leaked street address fragments if they appear late in the string
    m = _STREET_ADDR_RE.search(s)
    if m and m.start() >= max(10, len(s) // 2):
        s = s[:m.start()].strip()

    s = re.sub(r"\s+", " ", s).strip()
    return s


def primary_charge_category(description: str) -> str:
    """For 'Most common arrest category today:' take the first charge-like segment."""
    if not description:
        return ""
    s = description.strip()

    # split common separators for multiple charges
    parts = re.split(r"(?:\s{2,}|,\s+|\s+/\s+)", s)
    first = parts[0].strip()

    # keep it readable
    return first[:80].strip()


def most_common_category(records: list[dict]) -> str:
    cats = []
    for r in records:
        d = clean_description(r.get("description", "") or "")
        cat = primary_charge_category(d)
        if cat:
            cats.append(cat)
    if not cats:
        return ""
    return Counter(cats).most_common(1)[0][0]


def extract_report_date(pdf: pdfplumber.PDF) -> datetime | None:
    """
    Try to find the report date from the PDF text:
    often appears as 'Report Date: 2/2/2026'
    """
    for page in pdf.pages[:2]:
        txt = page.extract_text() or ""
        m = re.search(r"REPORT DATE:\s*(\d{1,2}/\d{1,2}/\d{4})", txt.upper())
        if m:
            try:
                return datetime.strptime(m.group(1), "%m/%d/%Y")
            except Exception:
                pass
    return None


def is_name_start(line: str) -> bool:
    """
    Name lines usually look like:
    'LASTNAME, FIRST ...'
    Uppercase, comma present.
    """
    if not line:
        return False
    s = line.strip()
    if "," not in s:
        return False
    # avoid header rows
    up = s.upper()
    if "INMATE" in up and "NAME" in up:
        return False
    # must start with letters
    return bool(re.match(r"^[A-Z][A-Z'\- ]+,\s*[A-Z]", up))


def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    """
    Returns (report_date, records)
    Record = { name, address, book_in_date, description }
    """
    records = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        report_dt = extract_report_date(pdf)

        # fallback: use "today" if not found
        if report_dt is None:
            report_dt = datetime.now()

        current = None
        buffer_lines = []

        def flush_current():
            nonlocal current, buffer_lines, records
            if not current:
                buffer_lines = []
                return

            # Join buffered lines and try to locate date + description
            joined = " ".join([x.strip() for x in buffer_lines if x.strip()])
            joined = re.sub(r"\s+", " ", joined).strip()

            # Find first date occurrence (book-in date)
            m = _DATE_RE.search(joined)
            book_in_date = m.group(1) if m else ""

            # Address: collect from buffer_lines those that look like address-ish (starts with digit or has TX zip)
            addr_lines = []
            for ln in buffer_lines:
                t = ln.strip()
                if not t:
                    continue
                if re.match(r"^\d{1,6}\s+", t) or re.search(r"\bTX\s+\d{5}\b", t):
                    addr_lines.append(t)

            address = " ".join(addr_lines).strip()

            # Description is whatever is left after date (and after we remove the address bits)
            desc = joined

            # Remove name if it accidentally got duplicated
            if current["name"]:
                desc = desc.replace(current["name"], " ").strip()

            # Remove address occurrences from description
            if address:
                desc = desc.replace(address, " ").strip()

            # If date present, drop everything up to and including that date (often date appears before desc)
            if m:
                desc = desc[m.end():].strip()

            # Clean junk / leaked report footer / city zip in description
            desc = clean_description(desc)

            # Only keep if we have name + date or description
            current["address"] = address
            current["book_in_date"] = book_in_date
            current["description"] = desc

            records.append(current)
            current = None
            buffer_lines = []

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

            for ln in lines:
                up = ln.upper()

                # Skip obvious repeating headers quickly
                if any(p in up for p in _JUNK_PHRASES):
                    continue

                # Detect new record start
                if is_name_start(ln):
                    # flush previous
                    flush_current()
                    current = {"name": ln.strip(), "address": "", "book_in_date": "", "description": ""}
                    buffer_lines = []
                    continue

                # If we are in a record, collect lines
                if current:
                    buffer_lines.append(ln)

        # flush last
        flush_current()

    # Final cleanup: drop empty junk records
    cleaned = []
    for r in records:
        # Some PDFs have occasional artifacts; keep only meaningful rows
        if r.get("name") and (r.get("book_in_date") or r.get("description") or r.get("address")):
            # One more pass to ensure description is clean
            r["description"] = clean_description(r.get("description", "") or "")
            cleaned.append(r)

    return report_dt, cleaned


# -----------------------------
# HTML email
# -----------------------------
def build_html(report_date: datetime, arrest_date: datetime, records: list[dict]) -> str:
    total = len(records)
    shown = min(total, MAX_ROWS)
    top_cat = most_common_category(records)

    # Make sure name is orange/bold, address normal font, mono spacing in address line is ok
    # Columns: Name | Book In Date | Description
    rows_html = []
    for r in records[:MAX_ROWS]:
        name = (r.get("name") or "").strip()
        address = (r.get("address") or "").strip()
        book_in = (r.get("book_in_date") or "").strip()
        desc = (r.get("description") or "").strip()

        # Address formatting: normal font, smaller, slight opacity, mono-ish feel
        addr_html = ""
        if address:
            addr_html = f"""
              <div style="margin-top:6px; font-size:12px; color:#b9b9b9; letter-spacing:0.2px;">
                {escape_html(address)}
              </div>
            """

        rows_html.append(f"""
          <tr>
            <td style="padding:12px 10px; vertical-align:top; border-bottom:1px solid #2b2b2b;">
              <div style="font-weight:800; color:{ORANGE}; letter-spacing:0.5px;">
                {escape_html(name)}
              </div>
              {addr_html}
            </td>
            <td style="padding:12px 10px; vertical-align:top; border-bottom:1px solid #2b2b2b; color:#d7d7d7; width:110px;">
              {escape_html(book_in)}
            </td>
            <td style="padding:12px 10px; vertical-align:top; border-bottom:1px solid #2b2b2b; color:#e6e6e6;">
              {escape_html(desc)}
            </td>
          </tr>
        """)

    bookings_line = f"""
      <div style="margin-top:10px; font-size:14px; color:#b9b9b9;">
        Total bookings today:
        <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{ORANGE}; font-weight:800;">
          {total}
        </span>
      </div>
    """

    common_cat_line = ""
    if top_cat:
        common_cat_line = f"""
          <div style="margin-top:6px; font-size:14px; color:#b9b9b9;">
            Most common arrest category today:
            <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{ORANGE};">
              {escape_html(top_cat)}
            </span>
          </div>
        """

    # NOTE: You asked to remove the formatting disclaimer. Keep it simple.
    # Subheader must be a day behind header date.
    title_date = report_date.strftime("%-m/%-d/%Y") if hasattr(report_date, "strftime") else str(report_date)
    sub_date = arrest_date.strftime("%-m/%-d/%Y") if hasattr(arrest_date, "strftime") else str(arrest_date)

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
</head>
<body style="margin:0; padding:0; background:#0f0f10; color:#e8e8e8; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <div style="max-width:760px; margin:0 auto; padding:28px 18px;">
    <div style="background:#151516; border:1px solid #242424; border-radius:14px; padding:22px 20px;">
      <div style="font-size:44px; font-weight:900; letter-spacing:-0.6px; line-height:1.05;">
        Tarrant County Jail Report — {title_date}
      </div>

      <div style="margin-top:10px; font-size:22px; color:#bdbdbd;">
        Summary of arrests in Tarrant County for {sub_date}
      </div>

      <div style="margin-top:14px; height:1px; background:#2a2a2a;"></div>

      <div style="margin-top:14px; font-size:14px; color:#a9a9a9;">
        This report is automated from Tarrant County data.
      </div>

      <div style="margin-top:22px; font-size:32px; font-weight:900;">
        Booked-In (Last 24 Hours)
      </div>

      {bookings_line}
      {common_cat_line}

      <div style="margin-top:14px; font-size:13px; color:#8f8f8f;">
        Showing {shown} of {total} records.
      </div>

      <div style="margin-top:12px; overflow-x:auto;">
        <table style="width:100%; border-collapse:collapse; background:#111112; border:1px solid #2b2b2b; border-radius:10px; overflow:hidden;">
          <thead>
            <tr style="background:#1c1c1d;">
              <th style="text-align:left; padding:12px 10px; font-size:13px; color:#bdbdbd; border-bottom:1px solid #2b2b2b;">Name</th>
              <th style="text-align:left; padding:12px 10px; font-size:13px; color:#bdbdbd; border-bottom:1px solid #2b2b2b; width:110px;">Book In Date</th>
              <th style="text-align:left; padding:12px 10px; font-size:13px; color:#bdbdbd; border-bottom:1px solid #2b2b2b;">Description</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html) if rows_html else '<tr><td colspan="3" style="padding:14px; color:#9a9a9a;">No records</td></tr>'}
          </tbody>
        </table>
      </div>

    </div>
  </div>
</body>
</html>
"""
    return html


def escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


# -----------------------------
# Email send
# -----------------------------
def send_email(subject: str, html_body: str):
    smtp_user = env("SMTP_USER", required=True)
    smtp_pass = env("SMTP_PASS", required=True)
    to_email = env("TO_EMAIL", required=True)

    smtp_host = env("SMTP_HOST", default="smtp.gmail.com")
    smtp_port_raw = env("SMTP_PORT", default="465").strip()

    # If somebody accidentally sets SMTP_PORT to empty string, fall back cleanly
    smtp_port = 465
    if smtp_port_raw:
        try:
            smtp_port = int(smtp_port_raw)
        except ValueError:
            smtp_port = 465

    from_email = env("FROM_EMAIL", default=smtp_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    text_fallback = "Tarrant County Jail Report (HTML email)."

    msg.attach(MIMEText(text_fallback, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=60) as server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, [to_email], msg.as_string())


# -----------------------------
# Main
# -----------------------------
def main():
    booked_url = env("BOOKED_PDF_URL", default=DEFAULT_BOOKED_PDF_URL)

    pdf_bytes = download_pdf(booked_url)
    report_date, records = parse_booked_in(pdf_bytes)

    # You requested: header date is report date, subheader is arrests on prior day
    arrest_date = report_date - timedelta(days=1)

    subject = f"Tarrant County Jail Report — {report_date.strftime('%-m/%-d/%Y')}"
    html = build_html(report_date, arrest_date, records)

    send_email(subject, html)
    print(f"[ok] report_date={report_date.strftime('%m/%d/%Y')} total_records={len(records)} sent_to={env('TO_EMAIL')}")


if __name__ == "__main__":
    main()
