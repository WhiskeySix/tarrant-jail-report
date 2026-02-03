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
# Config / helpers
# -----------------------------

ORANGE = "#f4a261"
BG = "#111315"
CARD = "#1b1f23"
TEXT = "#d7d7d7"
MUTED = "#a8b0b7"
BORDER = "#2a2f34"

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

# Booking number (used when present)
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# City detection and TX/ZIP
TX_ZIP_RE = re.compile(r"\bTX\b\s+\d{5}\b", re.IGNORECASE)

# Street suffixes (address signal)
STREET_SUFFIX_RE = re.compile(
    r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|CT|CIR|PL|PKWY|HWY|WAY|TRL|TER|PKY|PKWY)\b",
    re.IGNORECASE
)

# Common header/footer junk that leaks into rows
JUNK_MARKERS = (
    "Inmates Booked In During the Past 24 Hours",
    "Report Date:",
    "Page:",
    "Inmate Name Identifier",
    "Inmate Name",
    "Identifier",
    "CID",
    "Book In Date",
    "Booking No.",
    "Description",
)

# Some PDFs stick "FORT WORTH TX 76155" directly onto the end of a charge line
TRAILING_CITY_TX_ZIP_RE = re.compile(r"\b([A-Z][A-Z \-']+)\s+TX\s+\d{5}\b")


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def looks_like_address(s: str) -> bool:
    su = (s or "").upper()

    # obvious header/footer junk is NOT an address
    if any(j.upper() in su for j in JUNK_MARKERS):
        return False

    # typical street number + suffix
    if re.search(r"\b\d{1,6}\b", su) and STREET_SUFFIX_RE.search(su):
        return True

    # TX + ZIP line
    if TX_ZIP_RE.search(su):
        return True

    # apt/unit markers
    if re.search(r"\b(APT|UNIT|#)\b", su):
        return True

    return False


def extract_city_from_address_lines(addr_lines: list[str]) -> str:
    """
    Try to pull city from address lines.
    Prefers lines that look like 'FORT WORTH TX 76102' or 'FORT WORTH TX'
    If not found, returns empty string.
    """
    # Search from bottom up (city usually later)
    for line in reversed(addr_lines or []):
        su = line.upper().strip()

        # If line has TX + ZIP, city is what comes before TX
        if TX_ZIP_RE.search(su):
            parts = su.split(" TX", 1)
            city_part = parts[0].strip()
            # If it also includes street number, strip that off
            # e.g. "1234 SOME ST FORT WORTH"
            tokens = city_part.split()
            # Heuristic: city is last 1-4 tokens, so keep last chunk after street-like stuff
            # We'll take everything after the last street suffix, if present
            # Otherwise take last 2 tokens minimum
            m = STREET_SUFFIX_RE.search(city_part)
            if m:
                # if there is a suffix, likely street line, so city is after it (rare)
                pass
            # best guess: take last up to 4 tokens
            city_tokens = tokens[-4:] if len(tokens) >= 2 else tokens
            city = " ".join(city_tokens).title()
            return city

        # If line is just a city (like "FORT WORTH")
        # It shouldn't have digits
        if su and not re.search(r"\d", su) and len(su) <= 30:
            # avoid picking up charge fragments like "DRIVING WHILE"
            if not STREET_SUFFIX_RE.search(su) and su not in ("TX",):
                return su.title()

    return ""


def clean_charge_text(text: str, person_name: str) -> str:
    """
    Force description to contain ONLY charge text:
    - Remove PDF headers/footers
    - Remove embedded inmate name repeats
    - Remove address fragments (street / city TX zip)
    - Collapse whitespace
    """
    t = (text or "").strip()
    if not t:
        return ""

    # Drop known junk blocks
    for j in JUNK_MARKERS:
        t = t.replace(j, " ")

    # Remove repeated inmate name + variants
    if person_name:
        pn = person_name.upper().strip()
        t = re.sub(re.escape(pn), " ", t.upper()).strip()
        t = t.replace("  ", " ")

    # Remove booking numbers if they leaked into charge strings
    t = BOOKING_RE.sub(" ", t)

    # Remove street address fragments e.g. "2305 LENA ST" or "1517 CONNALLY TER"
    t = re.sub(r"\b\d{1,6}\s+[A-Z0-9'\- ]{2,35}\b(" + STREET_SUFFIX_RE.pattern + r")\b", " ", t, flags=re.IGNORECASE)

    # Remove "CITY TX 76123" tails
    t = re.sub(r"\b[A-Z][A-Z \-']+\s+TX\s+\d{5}\b", " ", t, flags=re.IGNORECASE)

    # Remove standalone TX + ZIP that might remain
    t = re.sub(r"\bTX\s+\d{5}\b", " ", t, flags=re.IGNORECASE)

    # Collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()

    # Keep uppercase look like your PDF
    return t.upper()


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
                # ignore obvious junk lines early
                if any(j in ln for j in JUNK_MARKERS):
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

                # Pattern B: CID DATE then NAME
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

                # if pending doesn't resolve cleanly, drop it
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
    Robust: will never crash and will separate address-ish vs charge-ish lines.
    Booking numbers (when present) anchor charge chunks.
    """

    # hard safety
    if "addr_lines" not in rec or rec["addr_lines"] is None:
        rec["addr_lines"] = []
    if "charges" not in rec or rec["charges"] is None:
        rec["charges"] = []

    # junk filter
    if any(j in ln for j in JUNK_MARKERS):
        return

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        # before first booking is likely address fragment
        pre = ln[: bookings[0].start()].strip()
        if pre and looks_like_address(pre):
            rec["addr_lines"].append(pre)

        # booking chunks become charges
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            if chunk:
                rec["charges"].append(chunk)
        return

    # No booking numbers:
    # Decide if address line or charge line
    if looks_like_address(ln):
        rec["addr_lines"].append(ln)
        return

    # If we have no charges yet, this is our first charge line
    if not rec["charges"]:
        rec["charges"].append(ln)
        return

    # Otherwise continuation of the last charge (wrapped line)
    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    # Clean addr lines
    addr_lines = []
    for a in rec.get("addr_lines", []) or []:
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2 and not any(j in a2 for j in JUNK_MARKERS):
            addr_lines.append(a2)

    # Build city: prefer extracted from addr lines; if still empty, try from charge lines (some PDFs embed city)
    city = extract_city_from_address_lines(addr_lines)

    # Clean charges and FORCE charge-only
    raw_charge = " ".join((rec.get("charges", []) or [])).strip()
    cleaned_charge = clean_charge_text(raw_charge, rec.get("name", ""))

    # If still empty (rare), salvage from any addr line that doesn't look like address
    if not cleaned_charge:
        # Sometimes the PDF gives charges as non-address lines but got misrouted — salvage them.
        salvage = []
        for a in addr_lines:
            if not looks_like_address(a):
                salvage.append(a)
        if salvage:
            cleaned_charge = clean_charge_text(" ".join(salvage), rec.get("name", ""))

    # If city is still empty, try to pull from the cleaned charge tail (CITY TX ZIP) before we stripped it (rare)
    if not city:
        m = TRAILING_CITY_TX_ZIP_RE.search(raw_charge.upper())
        if m:
            city = (m.group(1) or "").title().strip()

    # Final fallback
    if not city:
        city = "Unknown"

    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": city.strip(),
        "description": cleaned_charge.strip(),
    }


# -----------------------------
# HTML rendering
# -----------------------------

def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape(r.get("city", "")).strip()
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")
        date = html_escape(r.get("book_in_date", ""))

        name_block = f"""
          <div style="font-weight:800; color:{ORANGE}; letter-spacing:0.2px;">{name}</div>
          <div style="margin-top:6px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{TEXT}; font-size:13px; line-height:1.35;">
            {city}
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
    booked_day = env("BOOKED_DAY", "01").strip()  # keep simple

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
