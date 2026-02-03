import os
import re
import ssl
import smtplib
import urllib.request
from io import BytesIO
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pdfplumber


# ----------------------------
# Helpers
# ----------------------------

def env(name: str, default: str | None = None, required: bool = False) -> str:
    """
    Reads environment variable.
    Treats empty-string as missing.
    """
    val = os.getenv(name)
    if val is not None:
        val = val.strip()
        if val == "":
            val = None
    if val is None:
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return "" if default is None else default
    return val


def to_int(val: str, default: int) -> int:
    try:
        v = (val or "").strip()
        if v == "":
            return default
        return int(v)
    except Exception:
        return default


def download_pdf(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; TarrantJailReportBot/1.0)"
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


# ----------------------------
# Parsing: Booked-In PDF
# ----------------------------

RE_REPORT_DATE = re.compile(r"Report Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})")
RE_RECORD_START = re.compile(r"^(?P<name>.+?)\s+(?P<cid>\d{6,})\s+(?P<bookin>\d{1,2}/\d{1,2}/\d{4})\s*$")
RE_BOOKING_NO = re.compile(r"(\d{2}-\d{7})")
RE_CITY_STATE_ZIP = re.compile(r"^(.+?)\s+([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$")


def is_record_start(line: str) -> bool:
    return bool(RE_RECORD_START.match(line.strip()))


def parse_booked_in(pdf_bytes: bytes) -> tuple[str, list[dict]]:
    """
    Returns (report_date, records)
    record: {name, book_in_date, description, address}
    """
    report_date = ""
    records: list[dict] = []

    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

            # Pull report date (first one found wins)
            if not report_date:
                for ln in lines[:10]:
                    m = RE_REPORT_DATE.search(ln)
                    if m:
                        report_date = m.group(1)
                        break

            # Skip header lines until we pass the column header
            # Example header line: "Inmate Name Identifier CID Book In Date Booking No. Description"
            start_idx = 0
            for i, ln in enumerate(lines):
                if "Inmate Name" in ln and "Book In Date" in ln and "Description" in ln:
                    start_idx = i + 1
                    break

            i = start_idx
            while i < len(lines):
                ln = lines[i]

                m = RE_RECORD_START.match(ln)
                if not m:
                    i += 1
                    continue

                name = m.group("name").strip()
                # cid captured but NOT used in email
                book_in_date = m.group("bookin").strip()

                addr_line = ""
                city_line = ""
                description_parts: list[str] = []

                # Next lines contain:
                #   street + bookingNo + first description chunk
                #   city state zip
                j = i + 1

                # Collect until next record start or end of page
                while j < len(lines) and not is_record_start(lines[j]):
                    cur = lines[j]

                    # City/state/zip line?
                    if RE_CITY_STATE_ZIP.match(cur):
                        city_line = cur
                        j += 1
                        continue

                    # Line that contains booking no is usually: "<street> <bookingNo> <desc...>"
                    b = RE_BOOKING_NO.search(cur)
                    if b:
                        # Split around booking number
                        booking_no = b.group(1)
                        left = cur[:b.start()].strip()
                        right = cur[b.end():].strip()

                        if left and not addr_line:
                            addr_line = left

                        if right:
                            description_parts.append(right)

                        # After booking line, description may continue on subsequent lines
                        j += 1
                        while j < len(lines) and not is_record_start(lines[j]):
                            nxt = lines[j]
                            if RE_CITY_STATE_ZIP.match(nxt):
                                city_line = nxt
                                j += 1
                                continue
                            # If we hit another booking line unexpectedly, treat as continuation too
                            if RE_BOOKING_NO.search(nxt):
                                description_parts.append(nxt)
                                j += 1
                                continue
                            # Otherwise treat as description continuation
                            description_parts.append(nxt)
                            j += 1
                        break
                    else:
                        # Sometimes the street line may not include booking no (rare),
                        # but in this report the street is usually right here.
                        if not addr_line and not RE_CITY_STATE_ZIP.match(cur):
                            addr_line = cur
                        j += 1

                # Build full address
                full_address = ""
                if addr_line and city_line:
                    full_address = f"{addr_line}, {city_line}"
                elif addr_line:
                    full_address = addr_line
                elif city_line:
                    full_address = city_line

                description = " ".join(description_parts).strip()
                # Clean up double spaces
                description = re.sub(r"\s+", " ", description)

                records.append({
                    "name": name,
                    "book_in_date": book_in_date,
                    "description": description if description else "N/A",
                    "address": full_address if full_address else "N/A",
                })

                i = j

    if not report_date:
        report_date = "Unknown Date"

    return report_date, records


# ----------------------------
# HTML rendering
# ----------------------------

def build_html(report_date: str, records: list[dict], max_rows: int) -> tuple[str, str]:
    title = f"Tarrant County Jail Report — {report_date}"
    subtitle = f"Summary of arrests in Tarrant County for {report_date}"
    data_note = (
        "This report is automated from Tarrant County data. "
        "Some records may contain partial or wrapped fields due to source formatting."
    )

    total = len(records)
    shown = records[:max_rows]

    def esc(s: str) -> str:
        return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    rows_html = []
    for r in shown:
        name_html = f"<div style='font-weight:700;'>{esc(r['name'])}</div>"
        addr_html = (
            f"<div style='margin-top:4px; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "
            f"\"Liberation Mono\", \"Courier New\", monospace; font-size: 12px; color: #444;'>"
            f"{esc(r['address'])}</div>"
        )
        name_cell = name_html + addr_html

        rows_html.append(
            "<tr>"
            f"<td style='padding:10px; border-top:1px solid #e5e5e5; vertical-align:top;'>{name_cell}</td>"
            f"<td style='padding:10px; border-top:1px solid #e5e5e5; white-space:nowrap; vertical-align:top;'>{esc(r['book_in_date'])}</td>"
            f"<td style='padding:10px; border-top:1px solid #e5e5e5; vertical-align:top;'>{esc(r['description'])}</td>"
            "</tr>"
        )

    showing_line = (
        f"<div style='margin-top:6px; color:#555;'>Showing first {max_rows} of {total} records.</div>"
        if total > max_rows
        else ""
    )

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
</head>
<body style="margin:0; padding:0; background:#f6f6f6; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
  <div style="max-width:900px; margin:0 auto; padding:20px;">
    <div style="background:#ffffff; border:1px solid #eaeaea; border-radius:12px; padding:22px;">
      <div style="font-size:32px; font-weight:800; letter-spacing:-0.5px; margin:0 0 6px 0;">{esc(title)}</div>
      <div style="font-size:18px; color:#555; margin:0 0 14px 0;">{esc(subtitle)}</div>
      <hr style="border:none; border-top:1px solid #ededed; margin:16px 0;"/>
      <div style="color:#666; font-size:14px; line-height:1.4;">{esc(data_note)}</div>

      <div style="margin-top:18px; font-size:24px; font-weight:800;">Booked-In (Last 24 Hours)</div>
      <div style="margin-top:6px; color:#555;">{total} records parsed from Booked-In PDF.</div>
      {showing_line}

      <div style="overflow-x:auto; margin-top:14px;">
        <table style="width:100%; border-collapse:collapse; min-width:720px;">
          <thead>
            <tr style="background:#f1f1f1;">
              <th style="text-align:left; padding:10px; border:1px solid #e5e5e5;">Name</th>
              <th style="text-align:left; padding:10px; border:1px solid #e5e5e5; white-space:nowrap;">Book In Date</th>
              <th style="text-align:left; padding:10px; border:1px solid #e5e5e5;">Description</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows_html) if rows_html else "<tr><td colspan='3' style='padding:12px;'>No records</td></tr>"}
          </tbody>
        </table>
      </div>
    </div>
  </div>
</body>
</html>
""".strip()

    subject = title
    return subject, html


# ----------------------------
# Email
# ----------------------------

def send_email(subject: str, html_body: str) -> None:
    to_email = env("TO_EMAIL", required=True)

    smtp_host = env("SMTP_HOST", required=True)
    smtp_user = env("SMTP_USER", required=True)
    smtp_pass = env("SMTP_PASS", required=True)

    # Defaults that won't crash if blank
    smtp_port = to_int(env("SMTP_PORT", "465"), 465)
    smtp_mode = env("SMTP_MODE", "ssl").lower()  # "ssl" or "starttls"
    from_email = env("FROM_EMAIL", smtp_user)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if smtp_mode == "starttls":
        with smtplib.SMTP(smtp_host, smtp_port, timeout=60) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, [to_email], msg.as_string())
    else:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=60, context=ssl.create_default_context()) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, [to_email], msg.as_string())


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    booked_pdf_url = env(
        "BOOKED_PDF_URL",
        "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF",
        required=True,
    )
    max_rows = to_int(env("MAX_ROWS", "250"), 250)

    print(f"[booked-in] downloading: {booked_pdf_url}")
    pdf_bytes = download_pdf(booked_pdf_url)
    print(f"[booked-in] downloaded bytes: {len(pdf_bytes)}")

    report_date, records = parse_booked_in(pdf_bytes)
    print(f"[booked-in] report_date: {report_date}")
    print(f"[booked-in] parsed records: {len(records)}")

    subject, html_out = build_html(report_date, records, max_rows)

    print("[email] sending...")
    send_email(subject, html_out)
    print("[email] sent.")


if __name__ == "__main__":
    main()
