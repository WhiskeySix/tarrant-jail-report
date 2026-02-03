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
# Styling (ONLY)
# -----------------------------

# Dark (not black) intel-brief background
BG = "#0f1216"
CARD = "#151a21"
TABLE_BG = "#171c23"
BORDER = "#252b36"

TEXT = "#e6e8eb"
MUTED = "#9aa3ad"

# Purple “coding CLI” accent
ACCENT = "#b18cff"

# Fonts:
# - Headers / labels: mono intel vibe
# - Table content: highly readable
FONT_MONO = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"
FONT_BODY = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

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
# Parsing helpers / patterns
# -----------------------------

# Record header lines in the PDF extraction tend to show up as:
#   "LAST, FIRST MIDDLE   1234567   2/1/2026"
NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")

# Some PDFs include booking numbers, sometimes they don't
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# City/state/zip patterns (we use these for CITY under the name AND to strip from charges)
CITY_STATE_ZIP_RE = re.compile(r"^(?P<city>[A-Z][A-Z \-']+)\s+TX\s+(?P<zip>\d{5})(?:-\d{4})?$")
CITY_STATE_RE = re.compile(r"^(?P<city>[A-Z][A-Z \-']+)\s+TX(?:\s+\d{5}(?:-\d{4})?)?$")

# Street address-ish detection
STREET_SUFFIX_RE = re.compile(
    r"\b(AVE|AV|ST|DR|RD|LN|BLVD|CT|CIR|PKWY|HWY|TER|PL|WAY|TRL|LOOP|FWY|SQ|PARK|RUN|HOLW|HOLLOW|ROW|PT|PIKE|CV|COVE)\b"
)
LEADING_STREET_NUM_RE = re.compile(r"^\d{1,6}\s+")

# Strip trailing "... CITY TX 76123" that gets glued into charges
TRAILING_CITY_TX_ZIP_RE = re.compile(r"\s+([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?\s*$")

# Strip street address fragments inside a line of charges:
# "DRIVING WHILE INTOXICATED 2ND 2305 LENA ST"
# We remove the " 2305 LENA ST" chunk.
INLINE_STREET_ADDR_RE = re.compile(
    r"\s+\d{1,6}\s+[A-Z0-9][A-Z0-9 \-']{1,40}\s+(AVE|AV|ST|DR|RD|LN|BLVD|CT|CIR|PKWY|HWY|TER|PL|WAY|TRL|LOOP|FWY|SQ|CV|COVE)\b.*$"
)

# PDF boilerplate junk lines that sometimes leak into Description
JUNK_SUBSTRINGS = [
    "INMATES BOOKED IN DURING THE PAST",
    "REPORT DATE:",
    "PAGE:",
    "INMATE NAME IDENTIFIER",
    "CID",
    "BOOK IN DATE",
    "BOOKING NO.",
    "DESCRIPTION",
]


def is_junk_line(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return True
    for s in JUNK_SUBSTRINGS:
        if s in up:
            return True
    return False


def looks_like_address(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return False
    if CITY_STATE_ZIP_RE.match(up) or CITY_STATE_RE.match(up):
        return True
    if LEADING_STREET_NUM_RE.match(up):
        return True
    if STREET_SUFFIX_RE.search(up) is not None:
        return True
    return False


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text or "")
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def clean_charge_line(raw: str) -> str:
    """
    Make Description = charge only.
    Removes:
      - PDF boilerplate junk
      - trailing "CITY TX ZIP"
      - embedded street address chunks like "2305 LENA ST"
      - leftover orphan "TX 76123" patterns
    """
    if not raw:
        return ""

    s = normalize_ws(raw)

    # Drop boilerplate-y lines entirely
    if is_junk_line(s):
        return ""

    # Remove embedded street address tail if it appears (most common offender)
    s = INLINE_STREET_ADDR_RE.sub("", s).strip()

    # Remove trailing city/state/zip if it was glued on
    s = TRAILING_CITY_TX_ZIP_RE.sub("", s).strip()

    # If we still end with "TX 76123" (no city), drop it
    s = re.sub(r"\s+TX\s+\d{5}(?:-\d{4})?\s*$", "", s).strip()

    return s


def extract_city_from_addr_lines(addr_lines: list[str]) -> str:
    """
    We ONLY want the CITY under the name.
    Priority:
      1) A clean "CITY TX ZIP" line
      2) A "CITY TX" line
      3) If a line contains "... CITY TX 76123", attempt to pull CITY
      4) Unknown
    """
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

    # Sometimes extracted as "... FORT WORTH TX 76112" inside a longer line
    for ln in addr_lines:
        up = normalize_ws(ln).upper()
        m2 = re.search(r"([A-Z][A-Z \-']+)\s+TX\s+\d{5}(?:-\d{4})?$", up)
        if m2:
            return normalize_ws(m2.group(1).title())

    return "Unknown"


# -----------------------------
# Booked-In PDF parsing
# -----------------------------

def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None  # (cid, date)
    current = None  # record dict

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
                # Skip global junk early (prevents it ever leaking into charges)
                if is_junk_line(ln):
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

                # Pattern B: CID DATE then NAME line next
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

                # If pending doesn't resolve immediately, drop it (prevents name/charge bleed)
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
    Robust content routing:
      - Address lines -> addr_lines (we later extract CITY)
      - Charge lines -> charges (cleaned; no street/city/zip; no boilerplate)
    """
    # Key-safe (prevents your KeyError: 'charges')
    rec.setdefault("addr_lines", [])
    rec.setdefault("charges", [])

    s = normalize_ws(ln)
    if not s or is_junk_line(s):
        return

    # If line contains booking numbers, treat chunks after booking numbers as charges.
    bookings = list(BOOKING_RE.finditer(s))
    if bookings:
        # Anything before the first booking is usually address (street/city)
        pre = s[: bookings[0].start()].strip()
        if pre and looks_like_address(pre):
            rec["addr_lines"].append(pre)

        # Each booking chunk typically yields a charge chunk
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(s)
            chunk = s[start:end].strip(" -\t")
            chunk_clean = clean_charge_line(chunk)
            if chunk_clean:
                rec["charges"].append(chunk_clean)
        return

    # No booking number:
    # Route address-looking lines to addr_lines (even if charges already started)
    # BUT if we already have charges and this is a city line, just ignore it (do not append to charges).
    if looks_like_address(s):
        # If it's purely city/state/zip and charges started, keep it for city extraction but don't let it pollute charges
        rec["addr_lines"].append(s)
        return

    # Otherwise it's charge text or charge continuation
    cleaned = clean_charge_line(s)
    if not cleaned:
        return

    if not rec["charges"]:
        rec["charges"].append(cleaned)
    else:
        # continuation line – append to last charge
        rec["charges"][-1] = normalize_ws(rec["charges"][-1] + " " + cleaned)


def finalize_record(rec: dict) -> dict:
    charges = []
    for c in rec.get("charges", []):
        c2 = clean_charge_line(c)
        if c2:
            charges.append(c2)

    # Remove duplicates that can happen with wrap/boilerplate
    charges = [c for i, c in enumerate(charges) if c and c not in charges[:i]]

    addr_lines = []
    for a in rec.get("addr_lines", []):
        a2 = normalize_ws(a)
        if a2 and not is_junk_line(a2):
            addr_lines.append(a2)

    city = extract_city_from_addr_lines(addr_lines)

    # Description is ONLY charges (joined by newline)
    description = "\n".join(charges).strip()

    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": city,
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


def most_common_charge(booked_records: list[dict]) -> str:
    """
    Counts the FIRST charge line per record (already cleaned).
    """
    items = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first = desc.splitlines()[0].strip()
        first = normalize_ws(first).upper()
        if first:
            items.append(first)

    if not items:
        return "Unknown"

    top = Counter(items).most_common(1)[0][0]
    return top.title()


defdef render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # As requested: arrests date is 1 day behind header date
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    # ---- Most common charge (keep your existing logic exactly as-is) ----
    # IMPORTANT: This assumes your CURRENT working code already produces "description" as charge-only.
    # We are NOT changing parsing — only calculating a display stat from already-parsed descriptions.
    most_common_charge = ""
    try:
        all_charges = []
        for r in booked_records:
            desc = (r.get("description") or "").strip()
            if not desc:
                continue
            # If multiple charges separated by newlines, treat each line as its own item
            for line in desc.splitlines():
                line = line.strip()
                if line:
                    all_charges.append(line)
        if all_charges:
            most_common_charge = Counter(all_charges).most_common(1)[0][0]
    except Exception:
        most_common_charge = ""

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape(r.get("city", "") or r.get("address", "")).split("\n")[0].strip()
        date = html_escape(r.get("book_in_date", ""))

        # Description should already be charge-only from your locked parsing.
        desc_raw = (r.get("description") or "").strip()
        desc = html_escape(desc_raw).replace("\n", "<br>")

        # Name block: intel mono + purple for name; city smaller + muted
        name_block = f"""
          <div style="font-family:{FONT_MONO}; font-weight:800; color:{ACCENT}; letter-spacing:0.2px; font-size:14px;">
            {name}
          </div>
          <div style="margin-top:6px; font-family:{FONT_BODY}; color:{MUTED}; font-size:13px; line-height:1.35;">
            {city}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top;">{name_block}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; font-family:{FONT_BODY}; white-space:nowrap;">{date}</td>
            <td style="padding:14px 12px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; font-family:{FONT_BODY}; line-height:1.55;">{desc}</td>
          </tr>
        """)

    bookings_line = f"""
      <div style="margin-top:16px; padding:14px 16px; border-radius:12px; border:1px solid {BORDER}; background:#11151b; font-family:{FONT_MONO}; font-size:14px; color:{TEXT};">
        Total bookings in the last 24 hours:
        <span style="color:{ACCENT}; font-weight:900;">{total}</span>
        {"<br><span style='color:"+MUTED+";'>Most common charge:</span> <span style='color:"+ACCENT+"; font-weight:800;'>"+html_escape(most_common_charge)+"</span>" if most_common_charge else ""}
      </div>
    """

    source_line = f"""
      <div style="margin-top:18px; color:{MUTED}; font-size:14px; line-height:1.5; font-family:{FONT_MONO};">
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
  <div style="max-width:900px; margin:0 auto; padding:26px 18px 40px;">

    <div style="background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:22px 22px 18px;">

      <!-- Intel-style header -->
      <div style="font-family:{FONT_MONO}; font-size:44px; font-weight:900; letter-spacing:-0.6px; line-height:1.05; color:{TEXT};">
        Tarrant County Jail Report — {header_date_str}
      </div>

      <div style="margin-top:14px; display:flex; gap:12px; flex-wrap:wrap;">
        <div style="padding:8px 14px; border-radius:999px; background:#0b0e13; font-family:{FONT_MONO}; font-size:12px; color:{TEXT}; border:1px solid {BORDER};">
          UNCLASSIFIED // FOR INFORMATIONAL USE ONLY
        </div>
        <div style="padding:8px 14px; border-radius:999px; border:1px solid {BORDER}; font-family:{FONT_MONO}; font-size:12px; color:{TEXT};">
          SOURCE: TARRANT COUNTY (CJ REPORTS)
        </div>
      </div>

      <div style="margin-top:18px; border:1px solid {BORDER}; border-radius:12px; background:#11151b; overflow:hidden;">
        <div style="padding:14px 16px; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO}; color:{MUTED}; font-size:12px; letter-spacing:1.2px;">
          REPORT DATE
          <div style="margin-top:6px; color:{TEXT}; font-size:18px; font-weight:900; letter-spacing:0;">
            {header_date_str}
          </div>
        </div>
        <div style="padding:14px 16px; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO}; color:{MUTED}; font-size:12px; letter-spacing:1.2px;">
          ARRESTS DATE
          <div style="margin-top:6px; color:{TEXT}; font-size:18px; font-weight:900; letter-spacing:0;">
            {arrests_date}
          </div>
        </div>
        <div style="padding:14px 16px; font-family:{FONT_MONO}; color:{MUTED}; font-size:12px; letter-spacing:1.2px;">
          RECORDS
          <div style="margin-top:6px; color:{TEXT}; font-size:18px; font-weight:900; letter-spacing:0;">
            {total}
          </div>
        </div>
      </div>

      {source_line}

      <div style="margin-top:26px; font-family:{FONT_MONO}; font-size:34px; font-weight:900; letter-spacing:-0.3px; color:{TEXT};">
        Booked-In (Last 24 Hours)
      </div>

      {bookings_line}

      <div style="margin-top:18px; color:{MUTED}; font-size:14px; font-family:{FONT_MONO};">
        Showing first {shown} of {total} records.
      </div>

      <div style="margin-top:16px; overflow:hidden; border-radius:12px; border:1px solid {BORDER}; background:{TABLE_BG};">
        <table style="width:100%; border-collapse:collapse;">
          <thead>
            <tr style="background:#11151b;">
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO};">Name</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER}; width:120px; font-family:{FONT_MONO};">Book In Date</th>
              <th style="text-align:left; padding:12px; color:{MUTED}; font-weight:700; border-bottom:1px solid {BORDER}; font-family:{FONT_MONO};">Description</th>
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
    booked_day = env("BOOKED_DAY", "01").strip()  # simple: day 01 (you said: keep simple)

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
