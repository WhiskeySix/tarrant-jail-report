import os
import io
import re
import requests
import pdfplumber
from datetime import datetime
from collections import Counter
import smtplib
from email.mime.text import MIMEText


# -----------------------------
# REPORT SOURCES (Tarrant County CJ Reports)
# -----------------------------
BOOKED_IN_BASE = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/"
BONDS_DAY1_URL = "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF"

# Rolling window = 3 days of Booked-In PDFs (01, 02, 03)
BOOKED_IN_DAYS = ["01.PDF", "02.PDF", "03.PDF"]


# -----------------------------
# Helpers
# -----------------------------
def fetch_pdf(url: str) -> bytes | None:
    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.content
    except Exception:
        return None


def extract_lines_from_pdf(pdf_bytes: bytes) -> list[str]:
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = ln.strip()
                if ln:
                    lines.append(re.sub(r"\s+", " ", ln))
    return lines


def looks_like_name(line: str) -> bool:
    # "LAST, FIRST MIDDLE"
    return bool(re.match(r"^[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?$", line))


def is_probably_address(line: str) -> bool:
    # Address/city/zip lines that can be ALL CAPS and confuse parsers
    if " TX " in line:
        return True
    if re.search(r"\b\d{5}\b", line):
        return True
    street_tokens = [
        " ST", " AVE", " RD", " DR", " LN", " BLVD", " HWY", " PKWY",
        " CIR", " CT", " TRL", " PL", " TER", " WAY", " LOOP"
    ]
    if any(tok in line for tok in street_tokens) and re.search(r"\b\d+\b", line):
        return True
    return False


def html_escape(s: str) -> str:
    return (
        s.replace("&", "&amp;")
         .replace("<", "&lt;")
         .replace(">", "&gt;")
         .replace('"', "&quot;")
         .replace("'", "&#39;")
    )


# -----------------------------
# Parse: Booked-In PDFs
# We key by CID (best for matching)
# -----------------------------
def parse_booked_in(lines: list[str], source_day: str) -> list[dict]:
    """
    Returns records like:
    {
      cid, name, book_in_date, booking_no, charges(list), top_charge, source_day
    }
    """
    records = []

    # Pattern seen in your screenshots:
    # NAME  CID  2/1/2026  26-0259185  DESCRIPTION...
    row_pat = re.compile(
        r"^(?P<name>[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?)\s+"
        r"(?P<cid>\d{6,7})\s+"
        r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
        r"(?P<booking>\d{2}-\d{7})\s+"
        r"(?P<desc>.+)$"
    )

    # Some PDFs break description onto following line(s). We'll accumulate by (cid, booking).
    by_key = {}

    for ln in lines:
        m = row_pat.match(ln)
        if not m:
            continue

        name = m.group("name").strip()
        cid = m.group("cid").strip()
        date = m.group("date").strip()
        booking = m.group("booking").strip()
        desc = m.group("desc").strip()

        if not desc or is_probably_address(desc):
            continue

        key = (cid, booking)
        if key not in by_key:
            by_key[key] = {
                "cid": cid,
                "name": name,
                "book_in_date": date,
                "booking_no": booking,
                "charges": [],
                "source_day": source_day
            }
        by_key[key]["charges"].append(desc)

    # finalize
    for key, rec in by_key.items():
        # dedupe charges
        seen = set()
        charges = []
        for c in rec["charges"]:
            if c not in seen:
                seen.add(c)
                charges.append(c)
        rec["charges"] = charges
        rec["top_charge"] = charges[0] if charges else ""
        records.append(rec)

    return records


# -----------------------------
# Parse: Bonds Issued Day 1
# Key by CID (per your screenshot)
# -----------------------------
def parse_bonds_issued(lines: list[str]) -> list[dict]:
    """
    Bonds report columns (from your screenshot):
    Bond Number, Status, Amount, Court, CID, Name, Offense, MDate, Bondsmen...
    We'll extract: cid, name, offense, amount, mdate, bond_number
    """
    records = []

    amt_pat = re.compile(r"\b([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})\b")
    cid_pat = re.compile(r"\b(\d{6,7})\b")
    mdate_pat = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    bondno_pat = re.compile(r"^\b(\d{6,8})\b")  # bond number at start of row usually

    # We'll look for lines that contain: bondno + amount + cid + NAME + ... + mdate
    # Use a tolerant approach:
    # - bond number = first token if numeric
    # - amount = first money match
    # - cid = first 6/7 digit match AFTER amount often, but we’ll just take the first 6/7 digit match
    # - name = first "LAST, FIRST" pattern in the line
    # - mdate = last date in the line (magistration date)
    name_pat = re.compile(r"([A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+)")

    for ln in lines:
        # Skip headers
        if "List of Bonds Issued" in ln or ln.startswith("Bond Number") or ln.startswith("Page:"):
            continue

        amt_m = amt_pat.search(ln)
        cid_m = cid_pat.search(ln)
        name_m = name_pat.search(ln)
        dates = mdate_pat.findall(ln)
        bondno_m = bondno_pat.search(ln)

        if not (amt_m and cid_m and name_m and dates and bondno_m):
            continue

        bond_number = bondno_m.group(1)
        amount = amt_m.group(1)
        cid = cid_m.group(1)
        name = name_m.group(1)

        # Choose the last date on the line as MDate
        mdate = dates[-1]

        # Offense is the hardest: typically appears after the name and before the date(s).
        # We'll take substring between end of name and last date occurrence.
        name_end = name_m.end()
        last_date_pos = ln.rfind(mdate)
        offense = ln[name_end:last_date_pos].strip(" -|")

        # Clean offense: remove address-ish junk
        offense = re.sub(r"\s+", " ", offense).strip()
        if not offense or is_probably_address(offense):
            # still keep record, but mark offense unknown
            offense = "N/A"

        records.append({
            "bond_number": bond_number,
            "cid": cid,
            "name": name,
            "offense": offense,
            "amount": amount,
            "mdate": mdate
        })

    # Dedupe (same bond number repeated across pages)
    seen = set()
    out = []
    for r in records:
        k = (r["bond_number"], r["cid"], r["amount"], r["mdate"])
        if k in seen:
            continue
        seen.add(k)
        out.append(r)

    return out


# -----------------------------
# Email Builder
# -----------------------------
def build_email(today_str: str,
                booked_day1: list[dict],
                bonds_day1: list[dict],
                booked_rolling: list[dict],
                rolling_days: int = 3) -> str:

    # Summary: Booked Day 1
    total_booked = len(booked_day1)

    charge_counts = Counter()
    for r in booked_day1:
        if r.get("top_charge"):
            charge_counts.update([r["top_charge"]])
    top_charges = [c for c, _ in charge_counts.most_common(3)]
    top_charges_text = ", ".join(top_charges) if top_charges else "N/A"

    # Summary: Bonds Day 1
    total_bonds_set = len(bonds_day1)

    # Rolling match: Bonds Day 1 against Booked-In rolling window by CID
    rolling_by_cid = {}
    for r in booked_rolling:
        rolling_by_cid.setdefault(r["cid"], []).append(r)

    matched = []
    for b in bonds_day1:
        cid = b["cid"]
        if cid in rolling_by_cid:
            # pick the most recent-ish entry (best effort) — first is fine for email
            for booking in rolling_by_cid[cid]:
                matched.append({
                    "name": booking["name"],
                    "cid": cid,
                    "booking_no": booking.get("booking_no", ""),
                    "book_in_date": booking.get("book_in_date", ""),
                    "top_charge": booking.get("top_charge", ""),
                    "bond_amount": b.get("amount", ""),
                    "bond_mdate": b.get("mdate", ""),
                    "bond_offense": b.get("offense", "")
                })
                break

    matched_count = len(matched)

    # HTML tables (keep them readable, not huge)
    def table_row(cols):
        tds = "".join([f"<td style='padding:8px;border:1px solid #333'>{html_escape(str(c))}</td>" for c in cols])
        return f"<tr>{tds}</tr>"

    # Booked Day 1 table
    booked_rows = []
    for r in booked_day1[:60]:  # cap for email readability
        booked_rows.append(table_row([r["name"], r.get("booking_no",""), r.get("top_charge","")]))

    booked_table = f"""
    <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <tr style="background:#f0f0f0">
        <th align="left" style="padding:8px;border:1px solid #333">Name</th>
        <th align="left" style="padding:8px;border:1px solid #333">Booking No</th>
        <th align="left" style="padding:8px;border:1px solid #333">Top Charge</th>
      </tr>
      {''.join(booked_rows) if booked_rows else ''}
    </table>
    """

    # Bonds Day 1 table
    bonds_rows = []
    for b in bonds_day1[:60]:
        bonds_rows.append(table_row([b["name"], b.get("offense",""), b.get("amount",""), b.get("mdate","")]))
    bonds_table = f"""
    <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <tr style="background:#f0f0f0">
        <th align="left" style="padding:8px;border:1px solid #333">Name</th>
        <th align="left" style="padding:8px;border:1px solid #333">Offense (Bonds Report)</th>
        <th align="left" style="padding:8px;border:1px solid #333">Bond Set</th>
        <th align="left" style="padding:8px;border:1px solid #333">MDate</th>
      </tr>
      {''.join(bonds_rows) if bonds_rows else ''}
    </table>
    """

    # Matches table
    match_rows = []
    for m in matched[:60]:
        match_rows.append(table_row([
            m["name"],
            m.get("booking_no",""),
            m.get("book_in_date",""),
            m.get("top_charge",""),
            m.get("bond_amount",""),
            m.get("bond_mdate","")
        ]))

    matches_table = f"""
    <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <tr style="background:#f0f0f0">
        <th align="left" style="padding:8px;border:1px solid #333">Name</th>
        <th align="left" style="padding:8px;border:1px solid #333">Booking No</th>
        <th align="left" style="padding:8px;border:1px solid #333">Book In Date</th>
        <th align="left" style="padding:8px;border:1px solid #333">Top Charge</th>
        <th align="left" style="padding:8px;border:1px solid #333">Bond Set</th>
        <th align="left" style="padding:8px;border:1px solid #333">MDate</th>
      </tr>
      {''.join(match_rows) if match_rows else ''}
    </table>
    """

    html = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.45">
      <h2 style="margin:0 0 10px 0">Tarrant County Jail Report — {html_escape(today_str)}</h2>

      <h3 style="margin:16px 0 6px 0">1) New Bookings (Last 24 Hours)</h3>
      <p style="margin:0 0 6px 0"><b>{total_booked}</b> new bookings</p>
      <p style="margin:0 0 6px 0"><b>Top charges:</b> {html_escape(top_charges_text)}</p>
      {booked_table}

      <h3 style="margin:18px 0 6px 0">2) New Bonds Set (Last 24 Hours)</h3>
      <p style="margin:0 0 6px 0"><b>{total_bonds_set}</b> bonds set</p>
      {bonds_table}

      <h3 style="margin:18px 0 6px 0">3) Rolling Match (Bonds Set vs. Booked-In Last {rolling_days} Days)</h3>
      <p style="margin:0 0 6px 0"><b>{matched_count}</b> bond records matched to someone booked-in within the last {rolling_days} days (matched by CID)</p>
      {matches_table}

      <p style="color:#666;margin-top:14px">
        Note: The “Bonds Issued” report reflects bond amounts <b>set</b> in the last 24 hours. It does not indicate bond payment, release, or custody status.
        Matching is done by CID across a rolling {rolling_days}-day window because bond setting often occurs after booking.
      </p>
    </div>
    """
    return html


def send_email(subject: str, html_body: str):
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    to_email = os.environ["TO_EMAIL"]

    msg = MIMEText(html_body, "html")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
        s.login(smtp_user, smtp_pass)
        s.send_message(msg)


# -----------------------------
# Main
# -----------------------------
def main():
    # Pull Booked-In Day 1 (for "new bookings")
    booked_day1_pdf = fetch_pdf(BOOKED_IN_BASE + "01.PDF")
    booked_day1 = []
    if booked_day1_pdf:
        booked_day1 = parse_booked_in(extract_lines_from_pdf(booked_day1_pdf), source_day="01")

    # Pull Booked-In Day 1/2/3 (rolling window for matching)
    booked_rolling = []
    for day in BOOKED_IN_DAYS:
        pdf_bytes = fetch_pdf(BOOKED_IN_BASE + day)
        if not pdf_bytes:
            continue
        day_id = day.replace(".PDF", "")
        booked_rolling.extend(parse_booked_in(extract_lines_from_pdf(pdf_bytes), source_day=day_id))

    # Pull Bonds Issued Day 1 (last 24 hours)
    bonds_pdf = fetch_pdf(BONDS_DAY1_URL)
    bonds_day1 = []
    if bonds_pdf:
        bonds_day1 = parse_bonds_issued(extract_lines_from_pdf(bonds_pdf))

    today_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Tarrant County Jail Report — {today_str}"
    html = build_email(today_str, booked_day1, bonds_day1, booked_rolling, rolling_days=3)
    send_email(subject, html)


if __name__ == "__main__":
    main()
