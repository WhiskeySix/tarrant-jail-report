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

# Intel / CLI accent
PURPLE = "#7c3aed"  # CLI purple accent
INK = "#111827"     # near-black ink
MUTED_INK = "#4b5563"
PAPER = "#f5f3ee"   # dossier paper
PAPER_2 = "#fbfaf7"
LINE = "#d1d5db"
CHIP_BG = "#111827"
CHIP_TXT = "#f9fafb"

CODE_FONT = "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace"
SANS_FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif"

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
    pending = None
    current = None

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        first_text = pdf.pages[0].extract_text() or ""
        report_dt = extract_report_date_from_text(first_text) or datetime.now()

        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            for ln in lines:
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

    rec["charges"][-1] = (rec["charges"][-1] + " " + ln).strip()


def finalize_record(rec: dict) -> dict:
    charges = []
    for c in rec["charges"]:
        c2 = re.sub(r"\s+", " ", c).strip()
        if c2:
            charges.append(c2)

    addr_lines = []
    for a in rec["addr_lines"]:
        a2 = re.sub(r"\s+", " ", a).strip()
        if a2:
            addr_lines.append(a2)

    return {
        "name": rec["name"],
        "book_in_date": rec["book_in_date"],
        "address": "\n".join(addr_lines),
        "description": "\n".join(charges),
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
    arrests_date = (header_date - timedelta(days=1)).strftime("%-m/%-d/%Y")
    header_date_str = header_date.strftime("%-m/%-d/%Y")

    total = len(booked_records)
    shown = min(total, ROW_LIMIT)

    # Most common FULL charge (first line of description)
    charge_counts = {}

    def first_charge_line(desc: str) -> str | None:
        if not desc:
            return None
        line = desc.strip().splitlines()[0].strip()
        if not line:
            return None
        return re.sub(r"\s+", " ", line).upper()

    for r in booked_records:
        ch = first_charge_line(r.get("description", ""))
        if ch:
            charge_counts[ch] = charge_counts.get(ch, 0) + 1

    most_common_charge = (
        max(charge_counts.items(), key=lambda kv: kv[1])[0]
        if charge_counts else "Unknown"
    )

    rows_html = []
    for r in booked_records[:ROW_LIMIT]:
        name = html_escape(r["name"])
        addr = html_escape(r["address"]).replace("\n", "<br>")
        desc = html_escape(r["description"]).replace("\n", "<br>")
        date = html_escape(r["book_in_date"])

        name_block = f"""
          <div style="font-weight:900; color:{INK}; letter-spacing:0.2px; font-family:{CODE_FONT}; font-size:15px;">
            {name}
          </div>
          <div style="margin-top:6px; font-family:{CODE_FONT}; color:{MUTED_INK}; font-size:12.5px; line-height:1.35;">
            {addr}
          </div>
        """

        rows_html.append(f"""
          <tr>
            <td style="padding:14px 12px; border-top:1px solid {LINE}; vertical-align:top;">{name_block}</td>
            <td style="padding:14px 12px; border-top:1px solid {LINE}; vertical-align:top; color:{INK}; white-space:nowrap; font-family:{CODE_FONT}; font-size:13px;">{date}</td>
            <td style="padding:14px 12px; border-top:1px solid {LINE}; vertical-align:top; color:{INK}; font-family:{CODE_FONT}; font-size:13px; line-height:1.35;">{desc}</td>
          </tr>
        """)

    # Intel-style chips + briefing stats
    chips = f"""
      <div style="display:flex; gap:10px; flex-wrap:wrap; margin-top:14px;">
        <div style="background:{CHIP_BG}; color:{CHIP_TXT}; font-family:{CODE_FONT}; font-weight:900; font-size:12px; padding:7px 10px; border-radius:999px;">
          UNCLASSIFIED // FOR INFORMATIONAL USE ONLY
        </div>
        <div style="background:{PAPER_2}; color:{INK}; border:1px solid {LINE}; font-family:{CODE_FONT}; font-weight:800; font-size:12px; padding:7px 10px; border-radius:999px;">
          SOURCE: TARRANT COUNTY (CJ REPORTS)
        </div>
      </div>
    """

    brief_meta = f"""
      <div style="margin-top:14px; display:grid; grid-template-columns: 1fr 1fr 1fr; gap:10px;">
        <div style="background:{PAPER_2}; border:1px solid {LINE}; border-radius:12px; padding:10px 12px;">
          <div style="font-family:{CODE_FONT}; font-size:11px; color:{MUTED_INK};">REPORT DATE</div>
          <div style="font-family:{CODE_FONT}; font-weight:900; color:{INK};">{header_date_str}</div>
        </div>
        <div style="background:{PAPER_2}; border:1px solid {LINE}; border-radius:12px; padding:10px 12px;">
          <div style="font-family:{CODE_FONT}; font-size:11px; color:{MUTED_INK};">ARRESTS DATE</div>
          <div style="font-family:{CODE_FONT}; font-weight:900; color:{INK};">{arrests_date}</div>
        </div>
        <div style="background:{PAPER_2}; border:1px solid {LINE}; border-radius:12px; padding:10px 12px;">
          <div style="font-family:{CODE_FONT}; font-size:11px; color:{MUTED_INK};">RECORDS</div>
          <div style="font-family:{CODE_FONT}; font-weight:900; color:{INK};">{total}</div>
        </div>
      </div>
    """

    bookings_line = f"""
      <div style="margin-top:18px; padding:12px 14px; border:1px solid {LINE}; border-radius:12px; background:{PAPER_2};">
        <div style="font-family:{CODE_FONT}; font-size:14px; color:{MUTED_INK};">
          Total bookings in the last 24 hours:
          <span style="color:{PURPLE}; font-weight:900;">{total}</span>
        </div>
        <div style="margin-top:6px; font-family:{CODE_FONT}; font-size:14px; color:{MUTED_INK};">
          Most common charge:
          <span style="color:{PURPLE}; font-weight:900;">{html_escape(most_common_charge)}</span>
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
<body style="margin:0; padding:0; background:{PAPER}; color:{INK}; font-family:{SANS_FONT};">
  <div style="max-width:900px; margin:0 auto; padding:26px 18px 40px;">
    <div style="background:{PAPER_2}; border:1px solid {LINE}; border-radius:14px; padding:22px 22px 18px; box-shadow: 0 1px 0 rgba(0,0,0,0.04);">
      <div style="font-size:36px; font-weight:900; letter-spacing:-0.6px; line-height:1.1; font-family:{CODE_FONT}; color:{INK};">
        Tarrant County Jail Report — {header_date_str}
      </div>

      {chips}
      {brief_meta}

      <div style="margin-top:18px; height:1px; background:{LINE};"></div>

      <div style="margin-top:14px; color:{MUTED_INK}; font-size:13px; line-height:1.5; font-family:{CODE_FONT};">
        This report is automated from Tarrant County data.
      </div>

      <div style="margin-top:22px; font-size:28px; font-weight:900; letter-spacing:-0.3px; font-family:{CODE_FONT}; color:{INK};">
        Booked-In (Last 24 Hours)
      </div>

      {bookings_line}

      <div style="margin-top:12px; color:{MUTED_INK}; font-size:13px; font-family:{CODE_FONT};">
        Showing first {shown} of {total} records.
      </div>

      <div style="margin-top:16px; overflow:hidden; border-radius:12px; border:1px solid {LINE}; background:{PAPER_2};">
        <table style="width:100%; border-collapse:collapse; font-family:{CODE_FONT};">
          <thead>
            <tr style="background:#f1efe9;">
              <th style="text-align:left; padding:12px; color:{MUTED_INK}; font-weight:900; border-bottom:1px solid {LINE};">Name</th>
              <th style="text-align:left; padding:12px; color:{MUTED_INK}; font-weight:900; border-bottom:1px solid {LINE}; width:120px;">Book In Date</th>
              <th style="text-align:left; padding:12px; color:{MUTED_INK}; font-weight:900; border-bottom:1px solid {LINE};">Description</th>
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
    booked_day = env("BOOKED_DAY", "01").strip()

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Tarrant County Jail Report — {report_dt.strftime('%-m/%-d/%Y')}"
    html_out = render_html(report_dt, booked_records)
    send_email(subject, html_out)


if __name__ == "__main__":
    main()
