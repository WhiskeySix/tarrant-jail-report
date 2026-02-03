import os
import re
import ssl
import smtplib
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import pdfplumber


# -----------------------------
# Config
# -----------------------------
BOOKED_DAY1_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF"

TABLE_LIMIT = 150

TO_EMAIL = os.getenv("TO_EMAIL", "").strip()
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587


# -----------------------------
# PDF + parsing helpers
# -----------------------------
DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
CID_RE = re.compile(r"\b\d{6,8}\b")
BOOKING_RE = re.compile(r"\b\d{2}-\d{7}\b")

# Two record-start patterns seen in the PDF:
# 1) "LAST, FIRST ... <CID> <MM/DD/YYYY>"
NAME_CID_DATE_RE = re.compile(r"^(?P<name>.+?,.+?)\s+(?P<cid>\d{6,8})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")

# 2) "<CID> <MM/DD/YYYY>" then next line is "LAST, FIRST ..."
CID_DATE_ONLY_RE = re.compile(r"^(?P<cid>\d{6,8})\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})$")

# Name line in split-record case: "LAST, FIRST ..."
NAME_ONLY_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z][A-Z' \-]+.*$")


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def download_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extract_lines(pdf_bytes: bytes) -> list[str]:
    lines = []
    with pdfplumber.open(io=pdf_bytes) if False else None  # placeholder to satisfy linters


def pdf_to_lines(pdf_bytes: bytes) -> list[str]:
    lines: list[str] = []
    with pdfplumber.open(io=bytes_to_filelike(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = norm_space(ln)
                if ln:
                    # Skip repeated headers/column headers/page lines
                    if ln.startswith("Inmates Booked In During the Past 24 Hours"):
                        continue
                    if ln.startswith("Inmate Name Identifier CID"):
                        continue
                    if "Page:" in ln and "Report Date:" in ln:
                        continue
                    lines.append(ln)
    return lines


class bytes_to_filelike:
    """Minimal file-like wrapper so pdfplumber can open bytes."""
    def __init__(self, b: bytes):
        self._b = b
        self._i = 0

    def read(self, n: int = -1) -> bytes:
        if n == -1:
            n = len(self._b) - self._i
        chunk = self._b[self._i:self._i + n]
        self._i += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._i = offset
        elif whence == 1:
            self._i += offset
        elif whence == 2:
            self._i = len(self._b) + offset
        return self._i

    def tell(self) -> int:
        return self._i


def parse_report_date_from_pdf(pdf_bytes: bytes) -> str:
    # Look for "Report Date: M/D/YYYY" in the first page text
    with pdfplumber.open(io=bytes_to_filelike(pdf_bytes)) as pdf:
        if not pdf.pages:
            return ""
        text = pdf.pages[0].extract_text() or ""
        m = re.search(r"Report Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", text)
        return m.group(1) if m else ""


def extract_descriptions_from_line(line: str) -> list[str]:
    """
    Extract charge descriptions from any line that contains booking numbers.
    Example patterns:
      "123 MAIN ST 26-0259185 AGG ASSAULT W/DEADLY WEAPON"
      "FORT WORTH TX 76105 26-0259250 SEX OFFENDERS DUTY TO REGISTER..."
      "26-0259277 EVADING ARREST DETENTION"
    """
    descs: list[str] = []
    matches = list(BOOKING_RE.finditer(line))
    if not matches:
        return descs

    # For each booking match, take text after that booking number to end of line
    for mi, m in enumerate(matches):
        start = m.end()
        tail = norm_space(line[start:])
        if tail:
            descs.append(tail)
    return descs


def parse_booked_in_records(pdf_bytes: bytes) -> list[dict]:
    lines = pdf_to_lines(pdf_bytes)

    records: list[dict] = []
    current = None
    pending_cid = None
    pending_date = None

    def flush():
        nonlocal current
        if not current:
            return
        # finalize description (unique, preserve order)
        seen = set()
        cleaned = []
        for d in current.get("descs", []):
            d = norm_space(d)
            if d and d not in seen:
                seen.add(d)
                cleaned.append(d)
        current["Description"] = " | ".join(cleaned) if cleaned else "—"
        current["Name"] = current.get("Name") or "—"
        current["CID"] = current.get("CID") or "—"
        current["Book In Date"] = current.get("Book In Date") or "—"
        # drop internal list
        current.pop("descs", None)
        records.append(current)
        current = None

    for ln in lines:
        # Case A: Full record start on one line
        m_full = NAME_CID_DATE_RE.match(ln)
        if m_full:
            flush()
            current = {
                "Name": norm_space(m_full.group("name")),
                "CID": m_full.group("cid"),
                "Book In Date": m_full.group("date"),
                "descs": [],
            }
            pending_cid = None
            pending_date = None
            continue

        # Case B: CID+Date only line (split record)
        m_cd = CID_DATE_ONLY_RE.match(ln)
        if m_cd:
            # Don't flush here (next name line continues this record)
            flush()
            pending_cid = m_cd.group("cid")
            pending_date = m_cd.group("date")
            current = {
                "Name": "",  # will be filled by following name line
                "CID": pending_cid,
                "Book In Date": pending_date,
                "descs": [],
            }
            continue

        # If we are in split-record mode, next name line completes it
        if current and current.get("Name") == "" and pending_cid and pending_date:
            if NAME_ONLY_RE.match(ln):
                current["Name"] = ln
                pending_cid = None
                pending_date = None
                continue

        # Collect descriptions whenever we see booking number(s)
        if current:
            descs = extract_descriptions_from_line(ln)
            if descs:
                current["descs"].extend(descs)

    flush()
    return records


# -----------------------------
# Email rendering + sending
# -----------------------------
def build_email_html(report_date: str, rows: list[dict]) -> str:
    # Summary
    total = len(rows)

    # Top charges from Description (use first segment before " | " if multiple)
    charges = []
    for r in rows:
        d = r.get("Description", "")
        if not d or d == "—":
            continue
        # Take first charge chunk as "primary" for frequency ranking
        primary = d.split("|", 1)[0].strip()
        if primary:
            charges.append(primary)

    top3 = [c for c, _ in Counter(charges).most_common(3)]
    top_charges_text = ", ".join(top3) if top3 else "—"

    # Table rows (limit 150)
    display_rows = rows[:TABLE_LIMIT]

    def esc(s: str) -> str:
        return (
            (s or "")
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    table_trs = []
    for r in display_rows:
        table_trs.append(
            "<tr>"
            f"<td>{esc(r.get('Name','—'))}</td>"
            f"<td>{esc(r.get('CID','—'))}</td>"
            f"<td>{esc(r.get('Book In Date','—'))}</td>"
            f"<td>{esc(r.get('Description','—'))}</td>"
            "</tr>"
        )

    subtitle = f"Summary of arrests in Tarrant County for {report_date}"

    html = f"""\
<html>
<head>
  <style>
    body {{
      font-family: Arial, Helvetica, sans-serif;
      background: #0b0b0c;
      color: #f5f5f7;
      padding: 20px;
    }}
    h1 {{ margin: 0 0 6px 0; font-size: 28px; }}
    .sub {{ margin: 0 0 18px 0; color: #c7c7cc; }}
    .box {{
      background: #111114;
      border: 1px solid #2c2c2e;
      border-radius: 10px;
      padding: 14px;
      margin: 14px 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 10px;
    }}
    th, td {{
      border: 1px solid #2c2c2e;
      padding: 10px;
      vertical-align: top;
      font-size: 14px;
    }}
    th {{
      background: #1c1c1e;
      text-align: left;
    }}
    td {{
      background: #0f0f10;
    }}
    .note {{
      color: #c7c7cc;
      font-size: 13px;
      line-height: 1.4;
      margin-top: 10px;
    }}
  </style>
</head>
<body>
  <h1>Tarrant County Jail Report — {report_date}</h1>
  <p class="sub">{subtitle}</p>

  <div class="box">
    <b>1) Summary (Last 24 Hours)</b><br><br>
    <b>Total bookings:</b> {total}<br>
    <b>Top charges:</b> {esc(top_charges_text)}<br><br>
    <div class="note">
      This report is automated from Tarrant County data. Some records may contain partial or wrapped fields due to source formatting.
    </div>
  </div>

  <div class="box">
    <b>2) Booked-In Table (Last 24 Hours)</b><br>
    <div class="note">Showing up to {TABLE_LIMIT} records.</div>

    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>CID</th>
          <th>Book In Date</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        {''.join(table_trs) if table_trs else '<tr><td colspan="4">No records found in Day 01 PDF.</td></tr>'}
      </tbody>
    </table>
  </div>
</body>
</html>
"""
    return html


def send_email(subject: str, html_body: str):
    if not TO_EMAIL or not SMTP_USER or not SMTP_PASS:
        raise RuntimeError("Missing TO_EMAIL / SMTP_USER / SMTP_PASS. Check GitHub Secrets + workflow env block.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = TO_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=60) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, [TO_EMAIL], msg.as_string())


def main():
    print(f"[download] {BOOKED_DAY1_URL}")
    pdf_bytes = download_pdf(BOOKED_DAY1_URL)

    report_date = parse_report_date_from_pdf(pdf_bytes) or "Report Date Unavailable"
    print(f"[report_date] {report_date}")

    rows = parse_booked_in_records(pdf_bytes)
    print(f"[parsed] records={len(rows)}")

    html_body = build_email_html(report_date, rows)
    subject = f"Tarrant County Jail Report — {report_date}"

    print("[email] sending...")
    send_email(subject, html_body)
    print("[email] sent OK")


if __name__ == "__main__":
    main()
