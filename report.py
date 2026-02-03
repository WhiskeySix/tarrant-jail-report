#!/usr/bin/env python3
import os
import re
import ssl
import smtplib
import urllib.request
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pdfplumber


# ----------------------------
# Config
# ----------------------------

TABLE_LIMIT_DEFAULT = 150

BOOKING_NO_RE = re.compile(r"\b\d{2}-\d{7}\b")          # e.g. 26-0259188
DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")      # e.g. 2/1/2026
CID_RE = re.compile(r"\b\d{6,8}\b")                    # e.g. 1069424 (varies)
REPORT_DATE_RE = re.compile(r"Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})", re.I)

STREET_HINTS = {
    " RD", " DR", " ST", " AVE", " BLVD", " LN", " CT", " TRL", " WAY", " HWY", " PKWY",
    " CIR", " TER", " PL", " LOOP", " CV", " PKY"
}


def env(name: str, default: str | None = None, required: bool = False) -> str:
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v if v is not None else ""


def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def is_header_or_noise(line: str) -> bool:
    l = normalize_spaces(line)
    if not l:
        return True
    # Common headers / column headers
    header_phrases = [
        "Inmates Booked In During the Past 24 Hours",
        "Inmate Name",
        "Identifier CID",
        "Book In Date",
        "Booking No.",
        "Description",
        "Page:",
    ]
    return any(p.lower() in l.lower() for p in header_phrases)


def looks_like_address(line: str) -> bool:
    l = normalize_spaces(line)
    if not l:
        return False
    # Street address often starts with a number
    if l[0].isdigit():
        u = " " + l.upper()
        return any(h in u for h in STREET_HINTS)
    # City/state/zip line often includes TX and a zip
    if " TX " in (" " + l.upper() + " ") and re.search(r"\b\d{5}\b", l):
        return True
    return False


def looks_like_name_line(line: str) -> bool:
    l = normalize_spaces(line)
    if not l:
        return False
    # Name lines are usually uppercase and contain a comma
    # Avoid lines that clearly contain dates / booking numbers
    if DATE_RE.search(l) or BOOKING_NO_RE.search(l):
        return False
    if "," not in l:
        return False
    # Must have letters and be mostly uppercase (PDF extraction tends to uppercase)
    letters = re.sub(r"[^A-Za-z]", "", l)
    if len(letters) < 3:
        return False
    # Some lines may not be perfectly uppercase, so just require it not be "sentence case"
    return True


def clean_desc_piece(line: str) -> str:
    l = normalize_spaces(line)
    # Remove stray column artifacts
    return l


def extract_report_date(pages_text: list[str]) -> str:
    """
    Try to find "Report Date: M/D/YYYY" anywhere in the PDF text.
    Fallback to today's date (local runner time) if not found.
    """
    for t in pages_text:
        m = REPORT_DATE_RE.search(t)
        if m:
            return m.group(1)
    return datetime.now().strftime("%m/%d/%Y")


def download_pdf(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def parse_booked_in(pdf_bytes: bytes) -> tuple[str, list[dict]]:
    """
    Parse Booked-In PDF (JailedInmates FinalPDF/01.PDF) into records:
      Name | CID | Book In Date | Description

    Strategy:
      - Identify record boundaries by Booking No pattern (NN-NNNNNNN).
      - Look backward a few lines to find Name, CID, Date.
      - Build Description from the text on the booking line + subsequent lines
        until the next booking line.
      - Ignore obvious address lines and header lines.
    """
    pages_text = []
    all_lines: list[str] = []

    with pdfplumber.open(io=pdf_bytes) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            pages_text.append(text)
            lines = text.splitlines()
            for ln in lines:
                ln = ln.rstrip()
                if ln is None:
                    continue
                all_lines.append(ln)

    report_date = extract_report_date(pages_text)

    records: list[dict] = []
    current = None

    def close_current():
        nonlocal current
        if not current:
            return
        # Final cleanup
        current["name"] = normalize_spaces(current.get("name") or "—")
        current["cid"] = normalize_spaces(current.get("cid") or "—")
        current["book_in_date"] = normalize_spaces(current.get("book_in_date") or "—")
        desc = normalize_spaces(current.get("description") or "—")
        # Convert " | " duplication
        desc = re.sub(r"\s*\|\s*", " | ", desc)
        current["description"] = desc if desc else "—"
        records.append(current)
        current = None

    # Pre-clean lines: keep original order but remove obvious noise
    lines = [normalize_spaces(l) for l in all_lines if not is_header_or_noise(l)]

    for i, line in enumerate(lines):
        if not line:
            continue

        booking_match = BOOKING_NO_RE.search(line)
        if booking_match:
            # Start a new record (close previous)
            close_current()

            # Initialize
            current = {
                "name": "—",
                "cid": "—",
                "book_in_date": "—",
                "description": "",
            }

            # Description begins on this line after booking no (or whole line if extraction is weird)
            after = line[booking_match.end():].strip(" -|")
            if after:
                current["description"] = clean_desc_piece(after)

            # Look back up to 8 lines for Name/CID/Date
            name_idx = None
            for j in range(i - 1, max(-1, i - 9), -1):
                prev = lines[j]

                if current["book_in_date"] == "—":
                    dm = DATE_RE.search(prev)
                    if dm:
                        current["book_in_date"] = dm.group(0)

                if current["cid"] == "—":
                    # Prefer a digits-only CID line, but allow CID within a line
                    if prev.isdigit() and 6 <= len(prev) <= 8:
                        current["cid"] = prev
                    else:
                        cm = CID_RE.search(prev)
                        if cm and prev.strip() == cm.group(0):
                            current["cid"] = cm.group(0)

                if name_idx is None and looks_like_name_line(prev) and not looks_like_address(prev):
                    current["name"] = prev
                    name_idx = j

                if current["name"] != "—" and current["cid"] != "—" and current["book_in_date"] != "—":
                    break

            # Handle wrapped names like:
            #   "COLLINS," on one line and "DEMONTRION D" on the next line
            if name_idx is not None and normalize_spaces(current["name"]).endswith(","):
                # The continuation is usually the next line after the comma-line
                if name_idx + 1 < len(lines):
                    nxt = lines[name_idx + 1]
                    if nxt and nxt.upper() == nxt and ("," not in nxt) and not looks_like_address(nxt) and not DATE_RE.search(nxt) and not BOOKING_NO_RE.search(nxt):
                        current["name"] = normalize_spaces(current["name"] + " " + nxt)

            continue

        # If we are inside a record, accumulate description continuation lines
        if current:
            # Ignore addresses and obvious metadata
            if looks_like_address(line):
                continue
            if DATE_RE.search(line) and line.strip() == DATE_RE.search(line).group(0):
                continue
            if CID_RE.search(line) and line.strip().isdigit() and 6 <= len(line.strip()) <= 8:
                continue
            if looks_like_name_line(line):
                # Sometimes the PDF repeats a name line; don't treat as description
                continue

            # Keep lines that look like offenses/charges (often uppercase with symbols/digits)
            piece = clean_desc_piece(line)
            if piece:
                if current["description"]:
                    current["description"] += " | " + piece
                else:
                    current["description"] = piece

    close_current()
    return report_date, records


def build_html(report_date: str, records: list[dict], limit: int) -> str:
    title = f"Tarrant County Jail Report — {report_date}"
    subtitle = f"Summary of arrests in Tarrant County for {report_date}"
    data_note = (
        "This report is automated from Tarrant County data. "
        "Some records may contain partial or wrapped fields due to source formatting."
    )

    total = len(records)
    shown = records[:limit]

    # Basic, consistent, readable email HTML
    rows_html = []
    for r in shown:
        rows_html.append(
            "<tr>"
            f"<td>{escape_html(r['name'])}</td>"
            f"<td>{escape_html(r['cid'])}</td>"
            f"<td>{escape_html(r['book_in_date'])}</td>"
            f"<td>{escape_html(r['description'])}</td>"
            "</tr>"
        )

    table_html = (
        "<table>"
        "<thead><tr>"
        "<th>Name</th><th>CID</th><th>Book In Date</th><th>Description</th>"
        "</tr></thead>"
        "<tbody>"
        + "\n".join(rows_html if rows_html else ["<tr><td colspan='4'>No records found.</td></tr>"])
        + "</tbody></table>"
    )

    html = f"""\
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
      margin: 0;
      padding: 0;
      background: #0b0b0c;
      color: #f2f2f2;
    }}
    .wrap {{
      max-width: 980px;
      margin: 0 auto;
      padding: 24px 16px 40px;
    }}
    .card {{
      background: #121214;
      border: 1px solid #2a2a2f;
      border-radius: 14px;
      padding: 18px 18px 10px;
    }}
    h1 {{
      font-size: 28px;
      line-height: 1.2;
      margin: 0 0 8px;
    }}
    .sub {{
      font-size: 14px;
      color: #cfcfd6;
      margin: 0 0 14px;
    }}
    .note {{
      font-size: 12px;
      color: #a7a7b2;
      margin: 0 0 18px;
    }}
    .meta {{
      font-size: 13px;
      color: #cfcfd6;
      margin: 0 0 12px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
      border-radius: 12px;
      border: 1px solid #2a2a2f;
      background: #0f0f11;
    }}
    th, td {{
      text-align: left;
      vertical-align: top;
      padding: 10px 10px;
      border-bottom: 1px solid #232329;
      border-right: 1px solid #232329;
      font-size: 13px;
    }}
    th:last-child, td:last-child {{
      border-right: none;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    th {{
      background: #16161a;
      color: #f2f2f2;
      font-weight: 650;
      font-size: 13px;
    }}
    td {{
      color: #e8e8ee;
    }}
    .footer {{
      font-size: 12px;
      color: #9a9aa6;
      margin-top: 14px;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>{escape_html(title)}</h1>
      <p class="sub">{escape_html(subtitle)}</p>
      <p class="note">{escape_html(data_note)}</p>
      <p class="meta"><strong>Records parsed:</strong> {total} &nbsp; | &nbsp; <strong>Displayed:</strong> {min(total, limit)}</p>
      {table_html}
      <p class="footer">Source: Tarrant County CJ Reports (Booked-In PDF Day 01).</p>
    </div>
  </div>
</body>
</html>
"""
    return html


def escape_html(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def send_email(subject: str, html_body: str):
    to_email = env("TO_EMAIL", required=True)
    smtp_user = env("SMTP_USER", required=True)
    smtp_pass = env("SMTP_PASS", required=True)

    # Optional overrides
    smtp_host = env("SMTP_HOST", default="smtp.gmail.com")
    smtp_port = int(env("SMTP_PORT", default="587"))
    from_email = env("FROM_EMAIL", default=smtp_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email

    msg.attach(MIMEText(html_body, "html", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.sendmail(from_email, [to_email], msg.as_string())

    print(f"[email] Sent to {to_email} via {smtp_host}:{smtp_port} as {smtp_user}")


def main():
    # Only use Booked-In Day 01
    booked_base = env("BOOKED_BASE_URL", required=True).rstrip("/")
    booked_url = f"{booked_base}/01.PDF"

    table_limit = int(env("TABLE_LIMIT", default=str(TABLE_LIMIT_DEFAULT)))

    print(f"[booked-in] downloading: {booked_url}")
    pdf_bytes = download_pdf(booked_url)
    print(f"[booked-in] downloaded bytes: {len(pdf_bytes)}")

    report_date, records = parse_booked_in(pdf_bytes)
    print(f"[booked-in] parsed records: {len(records)} (showing up to {table_limit})")
    print(f"[booked-in] report date: {report_date}")

    subject = f"Tarrant County Jail Report — {report_date}"
    html = build_html(report_date, records, table_limit)

    # If email fails, raise and fail workflow
    send_email(subject, html)


if __name__ == "__main__":
    main()
