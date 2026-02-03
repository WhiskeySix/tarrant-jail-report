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

ORANGE = "#f4a261"  # use your orange vibe (matches your screenshot vibe closely)
BG = "#111315"
CARD = "#1b1f23"
TEXT = "#d7d7d7"
MUTED = "#a8b0b7"
BORDER = "#2a2f34"

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
# PDF Parsing (Booked-In)
# -----------------------------

NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")
CITY_STATE_ZIP_RE = re.compile(r"\b([A-Z][A-Z ]+?)\s+TX\s+(\d{5})\b")
STREET_RE = re.compile(
    r"\b\d{1,6}\s+[A-Z0-9#'./-]+\s+(?:AVE|AV|ST|RD|DR|LN|PL|CT|BLVD|HWY|PKWY|WAY|TRL|CIR|TER|PK|LOOP|FWY)\b",
    re.IGNORECASE
)

def extract_city(addr_lines: list[str]) -> str:
    """
    From address lines, return CITY only (usually found in line like: 'FORT WORTH TX 76131').
    If not found, return empty string.
    """
    for ln in reversed(addr_lines):
        m = CITY_STATE_ZIP_RE.search(ln.upper())
        if m:
            return m.group(1).strip()
    return ""

def extract_report_date_from_text(text: str) -> datetime | None:
    # Finds first date like 2/2/2026 on the first page header if present
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
        # report date from first page text if possible
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

                # If we hit a new NAME line unexpectedly while pending exists, treat pending as junk
                if pending and not current and ln:
                    # don’t glue pending into someone else's description
                    # we drop pending if the expected NAME doesn’t come next
                    pending = None

                # Content lines (address/charges) for the current record
                if not current:
                    continue

                apply_content_line(current, ln)

        if current:
            records.append(finalize_record(current))

    return report_dt, records


def apply_content_line(rec: dict, ln: str) -> None:
    """
    Splits lines into address fragments and charges.
    Booking numbers (e.g., 26-0259229) are used as charge anchors.
    """
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

    # No booking number found; decide whether it's address or continuation
    # Heuristic: if we haven't collected any charges yet, lines with digits are usually address.
    if not rec["charges"]:
        rec["addr_lines"].append(ln)
        return

    # Otherwise, treat as continuation of the last charge (wrap lines)
    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    # Clean up charges: collapse excessive whitespace and strip trailing address/city garbage
    charges = []
    for c in rec["charges"]:
        c2 = re.sub(r"\s+", " ", c).strip()
        if not c2:
            continue

        up = c2.upper()

        # If a city/state/zip shows up at the end, strip it (e.g., "FORT WORTH TX 76131")
        m = CITY_STATE_ZIP_RE.search(up)
        if m:
            # if it's in the latter half of the string, assume it's spillover and cut it off
            if m.start() >= max(8, len(c2) // 2):
                c2 = c2[:m.start()].strip()

        # If a street address shows up at the end, strip it too (spillover)
        m2 = STREET_RE.search(c2)
        if m2 and m2.start() >= max(8, len(c2) // 2):
            c2 = c2[:m2.start()].strip()

        c2 = re.sub(r"\s+", " ", c2).strip()
        if c2:
            charges.append(c2)

    # Clean address lines
    addr_lines = []
    for a in rec["addr_lines"]:
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2:
            addr_lines.append(a2)

    # ✅ CITY ONLY (instead of full street address)
    city_only = extract_city(addr_lines)

    return {
        "name": rec["name"],
        "book_in_date": rec["book_in_date"],
        "address": city_only,              # <-- city only goes under the name
        "description": "\n".join(charges), # <-- charge text only
    }


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # As requested: arrests date is 1 day behind header date
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r["name"])
        addr = html_escape(r["address"]).replace("\n", "<br>")
        desc = html_escape(r["description"]).replace("\n", "<br>")
        date = html_escape(r["book_in_date"])

        # Name in orange + bold; address normal (code-ish font)
        name_block = f"""
          <div style="font-weight:800; color:{ORANGE}; letter-spacing:0.2px;">{name}</div>
          <div style="margin-top:6px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; color:{TEXT}; font-size:13px; line-height:1.35;">
            {addr}
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
    # If you later want rolling N days, we can do that — but you told me: keep it simple & don’t break shit.

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
