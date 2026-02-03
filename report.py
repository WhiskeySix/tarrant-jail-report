import os
import io
import re
import ssl
import smtplib
import urllib.request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import pdfplumber


# -----------------------------
# Helpers
# -----------------------------
def env(name: str, default=None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and (val is None or str(val).strip() == ""):
        raise RuntimeError(f"Missing required environment variable: {name}")
    return str(val) if val is not None else ""


def download_pdf(url: str) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; TarrantJailReport/1.0)",
            "Accept": "application/pdf,*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


# -----------------------------
# Booked-In parser (column position based)
# Output rows: Name | CID | Book In Date | Description
# -----------------------------
def parse_report_date(pdf: pdfplumber.PDF) -> str:
    # Look for "Report Date: m/d/yyyy" on the first page
    first = pdf.pages[0].extract_text() or ""
    m = re.search(r"Report Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", first)
    if m:
        return m.group(1)
    # fallback: unknown
    return "Unknown Date"


def rows_from_page(page: pdfplumber.page.Page):
    """
    Reconstruct row "cells" by x-position buckets.
    We use the header positions to set column boundaries.

    Expected headers:
      Inmate Name | Identifier CID | Book In Date | Booking No. | Description
    """
    words = page.extract_words() or []
    if not words:
        return []

    # Find header row by locating "Inmate" word
    header_candidates = [w for w in words if w.get("text") == "Inmate"]
    if not header_candidates:
        return []

    header_top = header_candidates[0]["top"]

    header_words = [w for w in words if abs(w["top"] - header_top) < 2.0]
    header_words = sorted(header_words, key=lambda w: w["x0"])

    # Column boundaries (tuned to your PDF layout)
    # name: <230, cid: 230-315, date: 315-395, booking: 395-465, desc: 465+
    bounds = [0, 230, 315, 395, 465, 2000]

    # Group words into physical rows by rounded top position
    rows = {}
    for w in words:
        top = round(w["top"], 1)
        rows.setdefault(top, []).append(w)

    out = []
    for top in sorted(rows.keys()):
        # skip header + page title lines
        if top <= header_top + 1:
            continue

        ws = sorted(rows[top], key=lambda w: w["x0"])
        cols = ["", "", "", "", ""]  # name, cid, date, booking, desc
        for w in ws:
            x = w["x0"]
            txt = w["text"]
            for i in range(5):
                if bounds[i] <= x < bounds[i + 1]:
                    cols[i] = (cols[i] + " " + txt).strip() if cols[i] else txt
                    break

        # ignore footer noise
        if cols[4].startswith("Page:") or cols[4].startswith("Report Date:"):
            continue

        out.append(cols)

    return out


def parse_booked_in(pdf_bytes: bytes):
    """
    Returns:
      report_date (str),
      records: list[dict] with keys: name, cid, book_in_date, description
    """
    records = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        report_date = parse_report_date(pdf)

        current = {"name": "", "cid": "", "book_in_date": ""}
        pending_desc_parts = []

        def flush_desc_into_record(desc: str):
            desc = " ".join(desc.split()).strip()
            if not desc:
                return
            # only save if we have the core identity fields
            if current["name"] and current["cid"] and current["book_in_date"]:
                records.append(
                    {
                        "name": current["name"],
                        "cid": current["cid"],
                        "book_in_date": current["book_in_date"],
                        "description": desc,
                    }
                )

        for page in pdf.pages:
            rows = rows_from_page(page)
            for name_col, cid_col, date_col, booking_col, desc_col in rows:
                name_col = name_col.strip()
                cid_col = cid_col.strip()
                date_col = date_col.strip()
                booking_col = booking_col.strip()
                desc_col = desc_col.strip()

                # Update name if present
                # Names look like: LASTNAME, FIRST MIDDLE
                if name_col and "," in name_col and len(name_col) >= 4:
                    # New person often starts here
                    current["name"] = name_col

                # Update CID + date if present
                if cid_col and re.fullmatch(r"\d{6,10}", cid_col) and date_col and re.fullmatch(
                    r"\d{1,2}/\d{1,2}/\d{4}", date_col
                ):
                    current["cid"] = cid_col
                    current["book_in_date"] = date_col

                # Description fragments sometimes appear alone on their own row
                if desc_col and not booking_col:
                    pending_desc_parts.append(desc_col)

                # Booking number often appears alone after the description row
                # When we see a booking number, we treat that as "end of this charge entry"
                if booking_col and re.fullmatch(r"\d{2}-\d{7}", booking_col):
                    # If description is on same row, include it too
                    if desc_col:
                        pending_desc_parts.append(desc_col)

                    full_desc = " ".join(pending_desc_parts).strip()
                    flush_desc_into_record(full_desc)
                    pending_desc_parts = []

        return report_date, records


# -----------------------------
# HTML + Email
# -----------------------------
def build_html(report_date: str, records, limit: int):
    subtitle = f"Summary of arrests in Tarrant County for {report_date}"
    data_note = (
        "This report is automated from Tarrant County data. "
        "Some records may contain partial or wrapped fields due to source formatting."
    )

    total = len(records)
    shown = records[:limit]
    trunc_note = ""
    if total > limit:
        trunc_note = f"<p style='margin:8px 0 0 0; color:#bbb;'>Showing first {limit} of {total} records.</p>"
    else:
        trunc_note = f"<p style='margin:8px 0 0 0; color:#bbb;'>Showing {total} records.</p>"

    # Table rows
    trs = []
    for r in shown:
        trs.append(
            "<tr>"
            f"<td>{html_escape(r['name'])}</td>"
            f"<td>{html_escape(r['cid'])}</td>"
            f"<td>{html_escape(r['book_in_date'])}</td>"
            f"<td>{html_escape(r['description'])}</td>"
            "</tr>"
        )

    table_html = f"""
    <table style="width:100%; border-collapse:collapse; margin-top:12px; font-family:Arial, sans-serif; font-size:14px;">
      <thead>
        <tr>
          <th style="text-align:left; border:1px solid #444; padding:10px; background:#222;">Name</th>
          <th style="text-align:left; border:1px solid #444; padding:10px; background:#222;">CID</th>
          <th style="text-align:left; border:1px solid #444; padding:10px; background:#222;">Book In Date</th>
          <th style="text-align:left; border:1px solid #444; padding:10px; background:#222;">Description</th>
        </tr>
      </thead>
      <tbody>
        {''.join(trs) if trs else '<tr><td colspan="4" style="border:1px solid #444; padding:10px;">No records found.</td></tr>'}
      </tbody>
    </table>
    """

    html = f"""
    <div style="background:#111; color:#eee; padding:22px; font-family:Arial, sans-serif;">
      <h1 style="margin:0; font-size:28px;">Tarrant County Jail Report — {report_date}</h1>
      <p style="margin:8px 0 0 0; color:#ccc; font-size:16px;">{html_escape(subtitle)}</p>

      <hr style="border:none; border-top:1px solid #333; margin:16px 0;" />

      <p style="margin:0 0 12px 0; color:#bbb; font-size:13px;">{html_escape(data_note)}</p>

      <h2 style="margin:18px 0 6px 0; font-size:20px;">Booked-In (Last 24 Hours)</h2>
      <p style="margin:0; color:#ddd;">{total} records parsed from Booked-In PDF</p>
      {trunc_note}

      {table_html}
    </div>
    """
    return html.strip()


def send_email(subject: str, html_body: str):
    to_email = env("TO_EMAIL", required=True)
    smtp_user = env("SMTP_USER", required=True)
    smtp_pass = env("SMTP_PASS", required=True)

    # Defaults that work for most SMTP providers (esp Gmail app-password)
    smtp_host = env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(env("SMTP_PORT", "465"))

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
    # You can provide BOOKED_PDF_URL directly, OR provide BOOKED_BASE_URL + BOOKED_DAY (default "01")
    booked_pdf_url = env("BOOKED_PDF_URL", "")
    if not booked_pdf_url:
        booked_base = env("BOOKED_BASE_URL", required=True).rstrip("/")
        booked_day = env("BOOKED_DAY", "01").zfill(2)
        booked_pdf_url = f"{booked_base}/{booked_day}.PDF"

    table_limit = int(env("TABLE_LIMIT", "150"))

    print(f"[booked-in] downloading: {booked_pdf_url}")
    pdf_bytes = download_pdf(booked_pdf_url)
    print(f"[booked-in] downloaded bytes: {len(pdf_bytes)}")

    report_date, records = parse_booked_in(pdf_bytes)
    print(f"[booked-in] report_date: {report_date}")
    print(f"[booked-in] parsed records: {len(records)}")

    html_out = build_html(report_date, records, limit=table_limit)

    # Write a copy for debugging in the Actions logs/artifacts
    with open("report.html", "w", encoding="utf-8") as f:
        f.write(html_out)

    subject = f"Tarrant County Jail Report — {report_date}"
    print("[email] sending...")
    send_email(subject, html_out)
    print("[email] sent successfully.")


if __name__ == "__main__":
    main()
