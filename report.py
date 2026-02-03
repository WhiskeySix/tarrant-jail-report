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

ORANGE = "#f4a261"
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


def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# -----------------------------
# PDF Parsing (Booked-In)
# -----------------------------

# Patterns coming out of the PDF text
NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")

BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")  # booking no like 26-0259xxx

# City/state/zip patterns we can reliably detect
CITY_STATE_ZIP_RE = re.compile(r"\b([A-Z][A-Z \-'.]+)\s+TX\s+(\d{5})\b", re.IGNORECASE)
CITY_STATE_RE = re.compile(r"\b([A-Z][A-Z \-'.]+)\s+TX\b", re.IGNORECASE)

# PDF header garbage that leaks into description sometimes
GARBAGE_HINTS = [
    "INMATES BOOKED IN DURING THE PAST 24 HOURS",
    "REPORT DATE:",
    "PAGE:",
    "INMATE NAME IDENTIFIER",
    "BOOK IN DATE",
    "BOOKING NO",
    "DESCRIPTION",
    "CID",
]


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
    pending = None  # (cid, date) when CID DATE line appears before NAME
    current = None

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
                        "charge_lines": [],
                    }
                    pending = None
                    continue

                # Pattern B: CID DATE then next line NAME
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
                        "charge_lines": [],
                    }
                    pending = None
                    continue

                # If we were pending and we didn't get a name next, drop pending (prevents gluing junk)
                if pending and not current:
                    # once any other line appears, pending is unreliable
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
    Booking numbers (e.g., 26-0259229) are used as charge anchors when present.
    If booking numbers are NOT present, we use heuristics:
      - address-ish lines go to addr
      - otherwise (especially before any charges exist) treat as first charge
    """

    # --- SAFETY: ensure keys exist (prevents KeyError: 'charges') ---
    if "addr_lines" not in rec or rec["addr_lines"] is None:
        rec["addr_lines"] = []
    if "charges" not in rec or rec["charges"] is None:
        rec["charges"] = []

    # ---- ignore common PDF header/footer junk that leaks into rows ----
    junk_markers = (
        "Inmates Booked In During the Past 24 Hours",
        "Report Date:",
        "Page:",
        "Inmate Name Identifier",
        "CID",
        "Book In Date",
        "Booking No.",
        "Description",
    )
    if any(j in ln for j in junk_markers):
        return

    def looks_like_address(s: str) -> bool:
        s_up = s.upper()

        # street number / unit patterns
        if re.search(r"\b\d{1,6}\b", s_up):
            if re.search(r"\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|LN|LANE|BLVD|CT|CIR|PL|PKWY|HWY|WAY)\b", s_up):
                return True

        # city/state/zip style tails
        if re.search(r"\bTX\b", s_up) and re.search(r"\b\d{5}\b", s_up):
            return True

        if re.search(r"\b(APT|UNIT|#)\b", s_up):
            return True

        return False

    bookings = list(BOOKING_RE.finditer(ln))
    if bookings:
        # Anything before the first booking looks like address (street or city line)
        pre = ln[: bookings[0].start()].strip()
        if pre:
            rec["addr_lines"].append(pre)

        # Parse each booking chunk as a charge
        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(ln)
            chunk = ln[start:end].strip(" -\t")
            if chunk:
                rec["charges"].append(chunk)
        return

    # No booking number found:
    if not rec["charges"]:
        # If it looks like an address line, store as address. Otherwise it's the FIRST charge.
        if looks_like_address(ln):
            rec["addr_lines"].append(ln)
        else:
            rec["charges"].append(ln)
        return

    # If we already have charges, decide if this is address leakage or charge continuation
    if looks_like_address(ln):
        rec["addr_lines"].append(ln)
        return

    # Otherwise, treat as continuation of the last charge (wrap lines)
    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def extract_city_from_lines(lines: list[str]) -> str:
    """
    Find the best city candidate from any collected lines.
    Prefer a clear 'CITY TX 76123' style line.
    """
    text = " \n ".join([ln.strip() for ln in lines if ln and ln.strip()])
    if not text:
        return ""

    m = CITY_STATE_ZIP_RE.search(text)
    if m:
        city = m.group(1).strip()
        return title_city(city)

    m2 = CITY_STATE_RE.search(text)
    if m2:
        city = m2.group(1).strip()
        return title_city(city)

    return ""


def title_city(city: str) -> str:
    """
    The PDF gives ALL CAPS often. Make it look normal-ish.
    """
    city = re.sub(r"\s+", " ", city).strip()
    if not city:
        return ""
    # Title-case but keep internal punctuation
    return " ".join([w.capitalize() if w.isalpha() else w[:1].capitalize() + w[1:].lower() for w in city.split(" ")])


def looks_like_street_address(ln: str) -> bool:
    """
    Street addresses almost always start with digits in this dataset.
    """
    return bool(re.match(r"^\d{1,6}\s+", ln.strip()))


def remove_city_zip_from_charge(text: str) -> str:
    """
    Remove trailing 'FORT WORTH TX 76102' style fragments from charges.
    Also remove ' TX 76102' if present.
    """
    t = text
    # remove full CITY TX ZIP
    t = re.sub(r"\b[A-Z][A-Z \-'.]+\s+TX\s+\d{5}\b", "", t, flags=re.IGNORECASE).strip()
    # remove trailing TX ZIP even if city already stripped
    t = re.sub(r"\bTX\s+\d{5}\b", "", t, flags=re.IGNORECASE).strip()
    # collapse whitespace
    t = re.sub(r"\s+", " ", t).strip()
    return t


def strip_pdf_garbage_lines(lines: list[str]) -> list[str]:
    """
    Remove header/footer junk lines that leak into extracted text.
    """
    cleaned = []
    for ln in lines:
        u = ln.upper().strip()
        if not u:
            continue

        # remove obvious PDF headers
        if any(h in u for h in GARBAGE_HINTS):
            continue

        # remove "Inmate Name" blocks that get repeated inside description
        if NAME_ONLY_RE.match(u):
            continue

        # remove "Date: 2/2/2026" etc that comes from header blobs
        if re.search(r"\bDATE:\s*\d{1,2}/\d{1,2}/\d{4}\b", u):
            continue

        # remove "PAGE: 3 OF 12"
        if re.search(r"\bPAGE:\s*\d+\s+OF\s+\d+\b", u):
            continue

        cleaned.append(ln.strip())
    return cleaned


def extract_clean_charges(addr_lines: list[str], charge_lines: list[str]) -> str:
    """
    Produce "Description" = charges only.
    Strategy:
      - split charge_lines into sublines
      - remove header junk
      - remove street-address-like lines
      - remove NAME lines
      - remove city/zip fragments
      - keep remaining as charge lines
    """
    raw = []
    for ln in charge_lines:
        if not ln:
            continue
        # explode to catch "wrapped" pieces
        parts = [p.strip() for p in re.split(r"\n| {2,}", ln) if p.strip()]
        raw.extend(parts)

    raw = strip_pdf_garbage_lines(raw)

    # If a line is actually an address, drop it from charges
    filtered = []
    for ln in raw:
        if looks_like_street_address(ln):
            continue
        if NAME_ONLY_RE.match(ln.strip().upper()):
            continue
        filtered.append(ln)

    # Join then scrub city/zip patterns
    joined = " ".join(filtered)
    joined = re.sub(r"\s+", " ", joined).strip()

    # Remove any city/zip fragments (FORT WORTH TX 761xx) that still leak
    joined = remove_city_zip_from_charge(joined)

    # Sometimes the charge string still contains a trailing address fragment without TX/ZIP,
    # but at least this will stop the big offenders. Keep it conservative (don’t over-strip).
    return joined.strip()


def finalize_record(rec: dict) -> dict:
    # Normalize collected lines
    addr_lines = [re.sub(r"\s+", " ", a).strip() for a in rec.get("addr_lines", []) if a and a.strip()]
    charge_lines = [a.strip() for a in rec.get("charge_lines", []) if a and a.strip()]

    # City extraction: look in address lines first, but fall back to charge lines too (PDF sometimes leaks)
    city = extract_city_from_lines(addr_lines)
    if not city:
        city = extract_city_from_lines(charge_lines)

    description = extract_clean_charges(addr_lines, charge_lines)

    return {
        "name": rec["name"],
        "book_in_date": rec["book_in_date"],
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


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # arrests date is 1 day behind header date
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape(r.get("city", "")).strip()
        desc = html_escape(r.get("description", "")).strip()
        date = html_escape(r.get("book_in_date", ""))

        city_block = ""
        if city:
            city_block = f"""
              <div style="margin-top:6px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{TEXT}; font-size:13px; line-height:1.35;">
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

    # safe defaults
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
    booked_day = env("BOOKED_DAY", "01").strip()  # simple & stable

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
