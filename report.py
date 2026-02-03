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
# SOURCES
# -----------------------------
BOOKED_IN_BASE = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/"
BONDS_DAY1_URL  = "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF"

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
    # "LAST, FIRST MIDDLE" (all caps)
    return bool(re.match(r"^[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?$", line))


def is_probably_address(line: str) -> bool:
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


def clean_person_name(raw: str) -> str:
    """
    Bonds PDF text extraction sometimes runs columns together.
    We force the name to be ONLY "LAST, FIRST [MIDDLE]" and drop anything after.
    """
    raw = re.sub(r"\s+", " ", raw).strip()

    # Find first "LAST, FIRST" occurrence
    m = re.search(r"\b([A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+)\b", raw)
    if not m:
        return raw[:60]  # fallback
    name = m.group(1).strip()

    # After comma, keep at most 2 tokens (FIRST [MIDDLE]) to avoid swallowing offense
    if "," in name:
        last, rest = name.split(",", 1)
        tokens = [t for t in rest.strip().split(" ") if t]
        rest = " ".join(tokens[:2])
        name = f"{last.strip()}, {rest}".strip().strip(",")

    return name


# -----------------------------
# Parse: Booked-In PDFs (CID-based, state machine)
# -----------------------------
def parse_booked_in(lines: list[str], source_day: str) -> list[dict]:
    """
    Booked-In report often appears as blocks:
      NAME
      ADDRESS / CITY
      CID  DATE  BOOKING  DESCRIPTION
      (maybe another CID/DATE/BOOKING/DESCRIPTION line under same name)

    We'll:
      - detect NAME lines
      - then capture any subsequent line that contains: CID + date + booking + desc
      - attach to the last seen name
    """
    records_by_key = {}  # (cid, booking_no) -> record

    cid_date_booking_desc = re.compile(
        r"\b(?P<cid>\d{6,7})\b\s+"
        r"(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
        r"(?P<booking>\d{2}-\d{7})\s+"
        r"(?P<desc>.+)$"
    )

    booking_desc_only = re.compile(
        r"\b(?P<booking>\d{2}-\d{7})\b\s+(?P<desc>.+)$"
    )

    current_name = None
    last_key = None

    for ln in lines:
        ln = ln.strip()

        # Skip obvious header lines
        if "Inmates Booked In During the Past 24 Hours" in ln:
            continue
        if ln.startswith("Report Date:") or ln.startswith("Page:"):
            continue

        # If we hit a new name, set context
        if looks_like_name(ln):
            current_name = ln
            last_key = None
            continue

        if not current_name:
            continue

        # Main capture line: CID + date + booking + desc
        m = cid_date_booking_desc.search(ln)
        if m:
            cid = m.group("cid")
            date = m.group("date")
            booking = m.group("booking")
            desc = m.group("desc").strip()

            if desc and not is_probably_address(desc):
                key = (cid, booking)
                rec = records_by_key.get(key)
                if not rec:
                    rec = {
                        "cid": cid,
                        "name": current_name,
                        "book_in_date": date,
                        "booking_no": booking,
                        "charges": [],
                        "source_day": source_day
                    }
                    records_by_key[key] = rec

                rec["charges"].append(desc)
                last_key = key
            continue

        # Sometimes description wraps, so we allow "booking + desc" on next line
        m2 = booking_desc_only.search(ln)
        if m2 and last_key:
            desc = m2.group("desc").strip()
            if desc and not is_probably_address(desc):
                records_by_key[last_key]["charges"].append(desc)
            continue

        # Another common wrap is a plain ALL CAPS charge line after we captured a booking
        if last_key and ln.isupper() and len(ln) > 10 and not is_probably_address(ln):
            records_by_key[last_key]["charges"].append(ln)

    # Finalize: dedupe charges, top_charge
    out = []
    for rec in records_by_key.values():
        seen = set()
        charges = []
        for c in rec["charges"]:
            c = re.sub(r"\s+", " ", c).strip()
            if c and c not in seen:
                seen.add(c)
                charges.append(c)
        rec["charges"] = charges
        rec["top_charge"] = charges[0] if charges else ""
        out.append(rec)

    return out


# -----------------------------
# Parse: Bonds Issued Day 1 (CID-based)
# -----------------------------
def parse_bonds_issued(lines: list[str]) -> list[dict]:
    """
    Extract: cid, name, offense, amount, mdate, bond_number
    NOTE: Bonds PDF does NOT align with bookings by "booking no"
          CID is our correct join key.
    """
    records = []

    amt_pat = re.compile(r"\b([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})\b")
    cid_pat = re.compile(r"\b(\d{6,7})\b")
    date_pat = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    bondno_pat = re.compile(r"^\b(\d{6,8})\b")

    # Identify a name somewhere in the line
    name_pat = re.compile(r"\b([A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+)\b")

    for ln in lines:
        if "List of Bonds Issued" in ln or ln.startswith("Bond Number") or ln.startswith("Page:"):
            continue

        amt_m = amt_pat.search(ln)
        cid_m = cid_pat.search(ln)
        bondno_m = bondno_pat.search(ln)
        dates = date_pat.findall(ln)
        name_m = name_pat.search(ln)

        if not (amt_m and cid_m and bondno_m and dates and name_m):
            continue

        bond_number = bondno_m.group(1)
        amount = amt_m.group(1)
        cid = cid_m.group(1)
        mdate = dates[-1]

        # Clean name (prevent swallowing offense text)
        name = clean_person_name(name_m.group(1))

        # Offense: take text after name, before the last date token
        name_pos_end = ln.find(name_m.group(1)) + len(name_m.group(1))
        last_date_pos = ln.rfind(mdate)
        offense = ln[name_pos_end:last_date_pos].strip(" -|")
        offense = re.sub(r"\s+", " ", offense).strip()

        # If offense extraction is junk, mark N/A (still keep record)
        if not offense or is_probably_address(offense):
            offense = "N/A"

        records.append({
            "bond_number": bond_number,
            "cid": cid,
            "name": name,
            "offense": offense,
            "amount": amount,
            "mdate": mdate
        })

    # Dedupe
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
# Email
# -----------------------------
def build_email(today_str: str,
                booked_day1: list[dict],
                bonds_day1: list[dict],
                booked_rolling: list[dict],
                rolling_days: int = 3) -> str:

    # Section 1: Booked Day 1
    total_booked = len(booked_day1)
    charge_counts = Counter()
    for r in booked_day1:
        if r.get("top_charge"):
            charge_counts.update([r["top_charge"]])
    top_charges = [c for c, _ in charge_counts.most_common(3)]
    top_charges_text = ", ".join(top_charges) if top_charges else "N/A"

    # Section 2: Bonds set (Day 1 bonds report)
    total_bonds_set = len(bonds_day1)

    # Section 3: Rolling matches by CID
    rolling_by_cid = {}
    for r in booked_rolling:
        rolling_by_cid.setdefault(r["cid"], []).append(r)

    matched = []
    for b in bonds_day1:
        cid = b["cid"]
        if cid in rolling_by_cid:
            # pick the first booking record in the rolling window
            booking = rolling_by_cid[cid][0]
            matched.append({
                "name": booking["name"],
                "booking_no": booking.get("booking_no", ""),
                "book_in_date": booking.get("book_in_date", ""),
                "top_charge": booking.get("top_charge", ""),
                "bond_amount": b.get("amount", ""),
                "bond_mdate": b.get("mdate", "")
            })

    matched_count = len(matched)

    # Tables
    def table_row(cols):
        tds = "".join([
            f"<td style='padding:8px;border:1px solid #333;vertical-align:top'>{html_escape(str(c))}</td>"
            for c in cols
        ])
        return f"<tr>{tds}</tr>"

    booked_rows = [table_row([r["name"], r.get("booking_no",""), r.get("top_charge","")]) for r in booked_day1[:80]]
    bonds_rows  = [table_row([b["name"], b.get("offense",""), b.get("amount",""), b.get("mdate","")]) for b in bonds_day1[:80]]
    match_rows  = [table_row([m["name"], m.get("booking_no",""), m.get("book_in_date",""), m.get("top_charge",""), m.get("bond_amount",""), m.get("bond_mdate","")]) for m in matched[:80]]

    booked_table = f"""
    <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <tr style="background:#f0f0f0">
        <th align="left" style="padding:8px;border:1px solid #333">Name</th>
        <th align="left" style="padding:8px;border:1px solid #333">Booking No</th>
        <th align="left" style="padding:8px;border:1px solid #333">Top Charge</th>
      </tr>
      {''.join(booked_rows)}
    </table>
    """

    bonds_table = f"""
    <table style="border-collapse:collapse;width:100%;margin-top:8px">
      <tr style="background:#f0f0f0">
        <th align="left" style="padding:8px;border:1px solid #333">Name</th>
        <th align="left" style="padding:8px;border:1px solid #333">Offense (Bonds Report)</th>
        <th align="left" style="padding:8px;border:1px solid #333">Bond Set</th>
        <th align="left" style="padding:8px;border:1px solid #333">MDate</th>
      </tr>
      {''.join(bonds_rows)}
    </table>
    """

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
      {''.join(match_rows)}
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
        Note: The “Bonds Issued” report reflects bond amounts <b>set</b> in the last 24 hours. It does not indicate bond payment,
        release, or custody status. Matching is done by CID across a rolling {rolling_days}-day window because bond setting often occurs after booking.
      </p>
    </div>
    """
    return html


def send_email(subject: str, html_body: str):
    smtp_user = os.environ["SMTP_USER"]
    smtp_pass = os.environ["SMTP_PASS"]
    to_email   = os.environ["TO_EMAIL"]

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
    # Day 1 bookings (for "new bookings last 24 hours")
    booked_day1_pdf = fetch_pdf(BOOKED_IN_BASE + "01.PDF")
    booked_day1 = []
    if booked_day1_pdf:
        booked_day1 = parse_booked_in(extract_lines_from_pdf(booked_day1_pdf), source_day="01")

    # Rolling 3-day bookings (for matching)
    booked_rolling = []
    for day in BOOKED_IN_DAYS:
        pdf_bytes = fetch_pdf(BOOKED_IN_BASE + day)
        if not pdf_bytes:
            continue
        day_id = day.replace(".PDF", "")
        booked_rolling.extend(parse_booked_in(extract_lines_from_pdf(pdf_bytes), source_day=day_id))

    # Bonds set last 24 hours (Day 1 bonds report)
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
