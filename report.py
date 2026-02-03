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


# =============================
# THEME (intel briefing / CLI)
# =============================
PURPLE = "#c084fc"   # CLI purple vibe
BG = "#0b0f14"
CARD = "#0f141b"
TEXT = "#e6e6e6"
MUTED = "#a7b0bb"
BORDER = "#212833"
PILL_BG = "#e6e6e6"
PILL_TEXT = "#0b0f14"

MONO = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"

DEFAULT_BOOKED_BASE_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF"
ROW_LIMIT = int(os.getenv("ROW_LIMIT", "250"))


# =============================
# ENV HELPERS
# =============================
def env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v if v is not None else default


def safe_int(v: str, default: int) -> int:
    try:
        v = (v or "").strip()
        return int(v) if v else default
    except Exception:
        return default


def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


# =============================
# PDF PARSING (Booked-In)
# =============================
NAME_CID_DATE_RE = re.compile(
    r"^(?P<name>[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+)\s+(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$"
)
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,7})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z0-9][A-Z0-9' \-]+$")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# city/state/zip patterns
CITY_STATE_ZIP_RE = re.compile(r"\b([A-Z][A-Z ]{1,35})\s+TX\s+(\d{5})\b")
STATE_ZIP_RE = re.compile(r"\bTX\s+(\d{5})\b")

# street suffixes to detect address lines
STREET_SUFFIX_RE = re.compile(
    r"\b(ST|AVE|RD|DR|LN|BLVD|CT|PL|PKWY|HWY|WAY|TRL|TER|CIR|LOOP|PKWY|FWY|RUN|BND|PT|CV|SQ|PK)\b"
)

# junk lines that appear in the PDF and must NEVER become charges
JUNK_SNIPPETS = (
    "INMATES BOOKED IN DURING THE PAST",
    "REPORT DATE:",
    "PAGE:",
    "INMATE NAME IDENTIFIER",
    "BOOKING NO.",
    "BOOK IN DATE",
    "NAME IDENTIFIER",
    "CID",
    "DESCRIPTION",
)


def extract_report_date_from_text(text: str) -> datetime | None:
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{4})", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m/%d/%Y")
    except Exception:
        return None


def is_junk_line(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return True
    # obvious boilerplate / headers
    for s in JUNK_SNIPPETS:
        if s in up:
            return True
    # column header row
    if up in ("INMATE NAME", "BOOK IN DATE", "BOOKING NO.", "DESCRIPTION", "INMATE NAME IDENTIFIER CID"):
        return True
    # "Page X of Y" style
    if re.search(r"\bPAGE\s+\d+\s+OF\s+\d+\b", up):
        return True
    return False


def looks_like_address(ln: str) -> bool:
    up = (ln or "").strip().upper()
    if not up:
        return False

    # contains an obvious street number + street suffix
    if re.search(r"\b\d{1,6}\b", up) and STREET_SUFFIX_RE.search(up):
        return True

    # contains TX + zip
    if CITY_STATE_ZIP_RE.search(up) or STATE_ZIP_RE.search(up):
        return True

    # common address artifacts
    if "#" in up and re.search(r"\bAPT\b|\bUNIT\b|\bSTE\b", up):
        return True

    return False


def clean_charge_text(s: str) -> str:
    """
    Remove city/state/zip and street-address fragments that sometimes leak into charge lines.
    This is text cleanup only; parsing structure is unchanged.
    """
    if not s:
        return ""

    up = re.sub(r"\s+", " ", s).strip()

    # Strip any appended city/state/zip anywhere in the string
    up = CITY_STATE_ZIP_RE.sub("", up)
    up = re.sub(r"\b[A-Z][A-Z ]{1,35}\s+TX\b", "", up)  # city + TX (no zip)
    up = re.sub(r"\bTX\s+\d{5}\b", "", up)

    # Strip street-address tail (only if a street suffix is present)
    # Example: "DRIVING WHILE INTOXICATED 3445 FRAZIER AVE"
    up = re.sub(r"\s+\d{1,6}\s+[A-Z0-9 ]{2,40}\s+(ST|AVE|RD|DR|LN|BLVD|CT|PL|PKWY|HWY|WAY|TRL|TER|CIR|LOOP)\b.*$", "", up)

    # Final whitespace normalize
    up = re.sub(r"\s+", " ", up).strip(" -\t")

    return up


def extract_city_from_address_lines(addr_lines: list[str]) -> str:
    """
    City under name: prefer CITY TX ZIP from any address line; fallback to CITY TX.
    """
    blob = " ".join([re.sub(r"\s+", " ", a).strip().upper() for a in (addr_lines or []) if a])
    if not blob:
        return ""

    m = CITY_STATE_ZIP_RE.search(blob)
    if m:
        return m.group(1).title().strip()

    m2 = re.search(r"\b([A-Z][A-Z ]{1,35})\s+TX\b", blob)
    if m2:
        return m2.group(1).title().strip()

    # Sometimes PDF shows just a city word on its own line (e.g., "ARLINGTON")
    # Use the last address line that is alphabetic and not junk.
    for a in reversed(addr_lines or []):
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2 and a2.isalpha() and len(a2) <= 30:
            return a2.title().strip()

    return ""


def parse_booked_in(pdf_bytes: bytes) -> tuple[datetime, list[dict]]:
    records: list[dict] = []
    pending = None  # (cid, date)
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
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

                if pending and not current:
                    # if the expected NAME didn't arrive, drop pending to avoid contamination
                    pending = None

                if not current:
                    continue

                apply_content_line(current, ln)

        if current:
            records.append(finalize_record(current))

    return report_dt, records


def apply_content_line(rec: dict, ln: str) -> None:
    """
    Robust line classification:
    - Booking-number anchored lines: split into charges
    - Non-booking lines: decide address vs charge using heuristics
    - Prevents PDF header garbage from ever becoming a charge
    - Captures charges even when NO booking numbers are present (fixes missing description rows)
    """
    # Safety: ensure keys exist (prevents KeyError: 'charges' if something weird gets passed)
    rec.setdefault("addr_lines", [])
    rec.setdefault("charges", [])

    if is_junk_line(ln):
        return

    up = ln.strip()

    # Booking-number anchored line: treat as charge anchors + optional address prefix
    bookings = list(BOOKING_RE.finditer(up))
    if bookings:
        pre = up[: bookings[0].start()].strip()
        if pre and looks_like_address(pre):
            rec["addr_lines"].append(pre)

        for i, b in enumerate(bookings):
            start = b.end()
            end = bookings[i + 1].start() if i + 1 < len(bookings) else len(up)
            chunk = up[start:end].strip(" -\t")
            chunk = clean_charge_text(chunk)
            if chunk:
                rec["charges"].append(chunk)
        return

    # No booking numbers:
    # If it looks like an address => address bucket
    if looks_like_address(up):
        rec["addr_lines"].append(up)
        return

    # Otherwise treat as a charge line.
    # If charges already exist, decide whether it's a new charge vs wrap.
    cleaned = clean_charge_text(up)
    if not cleaned:
        return

    if not rec["charges"]:
        # first non-address line becomes the first charge
        rec["charges"].append(cleaned)
        return

    # If line is short-ish and "headline-like", treat as a new charge;
    # else treat as continuation wrap.
    if len(cleaned) <= 55:
        rec["charges"].append(cleaned)
    else:
        rec["charges"][-1] = (rec["charges"][-1] + " " + cleaned).strip()


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
        if a2:
            addr_lines.append(a2)

    city = extract_city_from_address_lines(addr_lines)

    # Description: charges only (joined lines)
    description = "\n".join(charges).strip()

    return {
        "name": rec.get("name", "").strip(),
        "book_in_date": rec.get("book_in_date", "").strip(),
        "city": city,                 # <-- city shown under name
        "description": description,   # <-- charges only
    }


# =============================
# HTML RENDERING
# =============================
def html_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def most_common_charge(booked_records: list[dict]) -> str:
    """
    Exact-match most common charge string (after cleanup).
    This avoids the 'DRIVING WHILE' partial issue because we use the FULL cleaned charge line.
    """
    items = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first_line = desc.splitlines()[0].strip()
        first_line = clean_charge_text(first_line)
        if first_line:
            items.append(first_line)
    if not items:
        return ""
    return Counter(items).most_common(1)[0][0]


def render_html(header_date: datetime, booked_records: list[dict]) -> str:
    # Arrests date is 1 day behind header date
    arrests_dt = header_date - timedelta(days=1)
    report_date_str = header_date.strftime("%-m/%-d/%Y")
    arrests_date_str = arrests_dt.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    top_charge = most_common_charge(booked_records)

    # rows
    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r.get("name", ""))
        city = html_escape((r.get("city") or "").strip())
        date = html_escape(r.get("book_in_date", ""))
        desc = html_escape(r.get("description", "")).replace("\n", "<br>")

        name_block = f"""
          <div style="font-weight:900; color:{PURPLE}; font-family:{MONO}; letter-spacing:0.3px;">
            {name}
          </div>
          <div style="margin-top:8px; font-family:{MONO}; color:{MUTED}; font-size:13px;">
            {city if city else "&nbsp;"}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:16px 14px; border-top:1px solid {BORDER}; vertical-align:top;">{name_block}</td>
            <td style="padding:16px 14px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; white-space:nowrap; font-family:{MONO};">{date}</td>
            <td style="padding:16px 14px; border-top:1px solid {BORDER}; vertical-align:top; color:{TEXT}; font-family:{MONO}; line-height:1.45;">{desc}</td>
          </tr>
        """)

    top_stats = f"""
      <div style="margin-top:18px; display:flex; gap:12px; flex-wrap:wrap;">
        <div style="flex:1; min-width:220px; background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:14px 16px;">
          <div style="color:{MUTED}; font-family:{MONO}; font-size:12px; letter-spacing:1.5px;">REPORT DATE</div>
          <div style="margin-top:8px; color:{TEXT}; font-family:{MONO}; font-size:22px; font-weight:900;">{report_date_str}</div>
        </div>

        <div style="flex:1; min-width:220px; background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:14px 16px;">
          <div style="color:{MUTED}; font-family:{MONO}; font-size:12px; letter-spacing:1.5px;">ARRESTS DATE</div>
          <div style="margin-top:8px; color:{TEXT}; font-family:{MONO}; font-size:22px; font-weight:900;">{arrests_date_str}</div>
        </div>

        <div style="flex:1; min-width:220px; background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:14px 16px;">
          <div style="color:{MUTED}; font-family:{MONO}; font-size:12px; letter-spacing:1.5px;">RECORDS</div>
          <div style="margin-top:8px; color:{PURPLE}; font-family:{MONO}; font-size:26px; font-weight:1000;">{total}</div>
        </div>
      </div>
    """

    pills = f"""
      <div style="margin-top:14px; display:flex; gap:12px; flex-wrap:wrap;">
        <div style="background:{PILL_BG}; color:{PILL_TEXT}; border-radius:999px; padding:10px 14px; font-family:{MONO}; font-weight:900; letter-spacing:0.6px;">
          UNCLASSIFIED // FOR INFORMATIONAL USE ONLY
        </div>
        <div style="background:{CARD}; color:{TEXT}; border:1px solid {BORDER}; border-radius:999px; padding:10px 14px; font-family:{MONO}; font-weight:900; letter-spacing:0.6px;">
          SOURCE: TARRANT COUNTY (CJ REPORTS)
        </div>
      </div>
    """

    bookings_box = f"""
      <div style="margin-top:16px; background:{CARD}; border:1px solid {BORDER}; border-radius:14px; padding:14px 16px; font-family:{MONO};">
        <div style="color:{MUTED}; font-size:14px; line-height:1.55;">
          Total bookings in the last 24 hours:
          <span style="color:{PURPLE}; font-weight:1000;">{total}</span>
        </div>
        {"<div style='margin-top:10px; color:"+MUTED+"; font-size:14px; line-height:1.55;'>Most common charge: <span style='color:"+PURPLE+"; font-weight:900;'>"+html_escape(top_charge)+"</span></div>" if top_charge else ""}
      </div>
    """

    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Tarrant County Jail Report — {report_date_str}</title>
</head>
<body style="margin:0; padding:0; background:{BG}; color:{TEXT}; font-family:{MONO};">
  <div style="max-width:940px; margin:0 auto; padding:26px 16px 44px;">
    <div style="background:{CARD}; border:1px solid {BORDER}; border-radius:18px; padding:22px 22px 18px;">
      <div style="font-family:{MONO}; font-size:46px; font-weight:1000; letter-spacing:0.6px; line-height:1.05;">
        Tarrant County<br/>Jail Report — {report_date_str}
      </div>

      {pills}
      {top_stats}

      <div style="margin-top:18px; height:1px; background:{BORDER};"></div>

      <div style="margin-top:16px; color:{MUTED}; font-size:16px; line-height:1.6; font-family:{MONO};">
        This report is automated from Tarrant County data.
      </div>

      <div style="margin-top:26px; font-family:{MONO}; font-size:40px; font-weight:1000; letter-spacing:0.6px;">
        Booked-In (Last 24 Hours)
      </div>

      {bookings_box}

      <div style="margin-top:14px; color:{MUTED}; font-size:14px; font-family:{MONO};">
        Showing first {shown} of {total} records.
      </div>

      <div style="margin-top:16px; overflow:hidden; border-radius:14px; border:1px solid {BORDER};">
        <table style="width:100%; border-collapse:collapse; background:#0d1218;">
          <thead>
            <tr style="background:#0f151d;">
              <th style="text-align:left; padding:14px; color:{MUTED}; font-weight:900; border-bottom:1px solid {BORDER}; font-family:{MONO};">Name</th>
              <th style="text-align:left; padding:14px; color:{MUTED}; font-weight:900; border-bottom:1px solid {BORDER}; width:140px; font-family:{MONO};">Book In Date</th>
              <th style="text-align:left; padding:14px; color:{MUTED}; font-weight:900; border-bottom:1px solid {BORDER}; font-family:{MONO};">Description</th>
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


# =============================
# EMAIL SENDING
# =============================
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


# =============================
# MAIN
# =============================
def main():
    booked_base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    booked_day = env("BOOKED_DAY", "01").strip()  # keep it simple
    booked_url = f"{booked_base}/{booked_day}.PDF"

    pdf_bytes = fetch_pdf(booked_url)
    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
