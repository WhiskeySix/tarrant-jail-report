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

ORANGE = "#f4a261"  # use your orange vibe (matches your screenshot vibe closely)
BG = "#111315"
CARD = "#1b1f23"
TEXT = "#d7d7d7"
MUTED = "#a8b0b7"
BORDER = "#2a2f34"

DEFAULT_BOOKED_BASE_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "250"))

# Ignore these when they show up inside extracted text (header/footer noise)
NOISE_SUBSTRINGS = [
    "Inmates Booked In During the Past 24 Hours",
    "Inmate Name Identifier",
    "Booking No.",
    "Report Date:",
    "Page:",
    "Description",
    "Book In Date",
    "CID",
]

# Matches "FORT WORTH TX 76137" (city/state/zip) or "HURST TX 76053"
CITY_STATE_ZIP_RE = re.compile(r"\b([A-Z][A-Z ]+?)\s+TX\s+(\d{5})\b")

# Remove any trailing "... CITY TX 76137" from the *charge text*
TRAILING_CITY_ZIP_RE = re.compile(r"\s+[A-Z][A-Z ]+\s+TX\s+\d{5}\s*$")

# Remove the CJ header/footer block if it gets glued into a cell
HEADER_BLOCK_RE = re.compile(
    r"Inmates Booked In During the Past 24 Hours.*?(?:Description|$)",
    re.IGNORECASE | re.DOTALL
)


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


def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


# -----------------------------
# PDF Parsing (Booked-In)
# -----------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def is_noise_line(ln: str) -> bool:
    if not ln:
        return True
    for s in NOISE_SUBSTRINGS:
        if s.lower() in ln.lower():
            return True
    return False


def clean_charge_text(s: str) -> str:
    """
    Make Description contain ONLY the charge text.
    Removes:
      - CJ report header/footer blocks that get glued into the cell
      - trailing CITY TX ZIP
      - extra whitespace
    """
    if not s:
        return ""

    s = HEADER_BLOCK_RE.sub("", s)  # drop big header block if present
    for token in NOISE_SUBSTRINGS:
        # extra safety: remove fragments if they appear
        s = re.sub(re.escape(token), "", s, flags=re.IGNORECASE)

    s = re.sub(r"\s+", " ", s).strip()

    # Remove trailing "FORT WORTH TX 76137" style endings
    s = TRAILING_CITY_ZIP_RE.sub("", s).strip()

    return s


def extract_city_only(addr_lines: list[str]) -> str:
    """
    You said: show CITY only under name.
    We pull it from the address lines.
    Examples:
      "4837 THISTLEDOWN DR, FORT WORTH TX 76137" -> "FORT WORTH"
      "HURST TX 76053" -> "HURST"
    """
    # Search from bottom up: city is usually on the last line
    for line in reversed(addr_lines or []):
        line2 = re.sub(r"\s+", " ", (line or "")).strip().upper()
        m = CITY_STATE_ZIP_RE.search(line2)
        if m:
            return m.group(1).strip()

    # If we can’t find it, return empty (don’t print junk)
    return ""


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
                if is_noise_line(ln):
                    continue

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

                # If we hit other lines while pending exists, drop pending so it doesn’t pollute content
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
    Split into address fragments and charges.
    Booking numbers are charge anchors, but the extracted line can contain:
      - address fragments
      - charge fragments
      - city/zip fragments
      - header/footer fragments
    We sanitize later in finalize_record + clean_charge_text.
    """
    if is_noise_line(ln):
        return

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        # Anything before the first booking is usually address-ish
        pre = ln[: bookings[0].start()].strip()
        if pre and not is_noise_line(pre):
            rec["addr_lines"].append(pre)

        # Parse each booking chunk as a charge candidate
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            if chunk and not is_noise_line(chunk):
                rec["charges"].append(chunk)
        return

    # No booking number found:
    # If we have no charges yet, treat as address lines
    if not rec["charges"]:
        if not is_noise_line(ln):
            rec["addr_lines"].append(ln)
        return

    # Otherwise treat as continuation of last charge (wrap line)
    if not is_noise_line(ln):
        rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    # Clean address lines
    addr_lines = []
    for a in rec.get("addr_lines", []):
        a2 = re.sub(r"\s+", " ", (a or "")).strip()
        if a2 and not is_noise_line(a2):
            addr_lines.append(a2)

    # City only under name
    city_only = extract_city_only(addr_lines)

    # Clean charges
    charges_clean = []
    for c in rec.get("charges", []):
        c2 = clean_charge_text(c)
        if c2:
            charges_clean.append(c2)

    # If the PDF glued multiple charges together, keep them on new lines
    description = "\n".join(charges_clean)

    return {
        "name": rec["name"],
        "book_in_date": rec["book_in_date"],
        "city": city_only,
        "description": description,
    }


# -----------------------------
# HTML rendering
# -----------------------------

def most_common_category(booked_records: list[dict]) -> str:
    """
    "Most common arrest category today:"
    We'll define "category" as the first charge line (or first sentence chunk).
    Keeps it simple, stable, and won’t break your layout.
    """
    cats = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first_line = desc.split("\n", 1)[0].strip()
        if not first_line:
            continue
        # normalize spacing
        first_line = re.sub(r"\s+", " ", first_line).strip()
        cats.append(first_line)

    if not cats:
        return "N/A"

    top, _ = Counter(cats).most_common(1)[0]
    return top


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # arrests date is 1 day behind header date
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    top_category = most_common_category(booked_records)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r["name"])
        city = html_escape(r.get("city", ""))
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")
        date = html_escape(r["book_in_date"])

        # Name in orange + bold; city in normal monospace under it
        city_block = ""
        if city:
            city_block = f"""
              <div style="margin-top:6px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
                          color:{TEXT}; font-size:13px; line-height:1.35;">
                {city}
              </div>
            """

        name_block = f"""
          <div style="font-weight:800; color:{ORANGE}; letter-spacing:0.2px;">{name}</div>
          {city_block}
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
      <div style="margin-top:8px; font-size:15px; color:{MUTED};">
        Most common arrest category today:
        <span style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{TEXT}; font-weight:700;">
          {html_escape(top_category)}
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

    # Safe defaults so we don't break when you didn't define these
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
