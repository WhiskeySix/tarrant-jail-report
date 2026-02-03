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
BOOKED_IN_DAYS = ["01.PDF", "02.PDF", "03.PDF"]  # rolling 3-day window


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
    except Exception as e:
        print(f"[fetch_pdf] failed {url}: {e}")
        return None


def extract_lines_from_pdf(pdf_bytes: bytes) -> list[str]:
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = ln.strip()
                if ln:
                    # normalize internal whitespace
                    lines.append(re.sub(r"\s+", " ", ln))
    return lines


def debug_dump(lines: list[str], title: str, n: int = 80):
    print(f"\n===== DEBUG DUMP: {title} (first {n} lines) =====")
    for i, ln in enumerate(lines[:n], start=1):
        print(f"{i:03d}: {ln}")
    print("===== END DEBUG DUMP =====\n")


def looks_like_name(line: str) -> bool:
    # "LAST, FIRST [MIDDLE]" all caps
    return bool(re.match(r"^[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?$", line))


def is_probably_address(line: str) -> bool:
    if " TX " in line:
        return True
    if re.search(r"\b\d{5}\b", line):
        return True
    street_tokens = [" ST", " AVE", " RD", " DR", " LN", " BLVD", " HWY", " PKWY", " CIR", " CT", " TRL", " PL", " TER", " WAY", " LOOP"]
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
    raw = re.sub(r"\s+", " ", raw).strip()
    m = re.search(r"\b([A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+)\b", raw)
    if not m:
        return raw[:60]
    name = m.group(1).strip()

    # keep FIRST + optional MIDDLE only
    if "," in name:
        last, rest = name.split(",", 1)
        tokens = [t for t in rest.strip().split(" ") if t]
        rest = " ".join(tokens[:2])
        name = f"{last.strip()}, {rest}".strip().strip(",")

    return name


# -----------------------------
# Parse: Booked-In PDFs (FIXED)
# Handles split layout:
#   - NAME (maybe alone)
#   - CID (maybe on same line as name or another line)
#   - DATE + BOOKING + DESCRIPTION (later line)
#   - description wraps (later lines)
# -----------------------------
def parse_booked_in(lines: list[str], source_day: str) -> list[dict]:
    records_by_key = {}  # (cid, booking_no) -> record

    # Pattern when line contains name + cid (common)
    name_cid_pat = re.compile(r"^(?P<name>[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?)\s+(?P<cid>\d{6,7})\b")

    # Pattern when line contains date + booking + desc (CID might NOT be on this line)
    date_booking_desc_pat = re.compile(
        r"^(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+"
        r"(?P<booking>\d{2}-\d{7})\s+"
        r"(?P<desc>.+)$"
    )

    # Pattern when line contains CID alone (sometimes appears on its own line)
    cid_alone_pat = re.compile(r"^(?P<cid>\d{6,7})$")

    current_name = None
    current_cid = None
    last_key = None

    for ln in lines:
        # skip headers
        if "Inmates Booked In During the Past 24 Hours" in ln:
            continue
        if ln.startswith("Report Date:") or ln.startswith("Page:"):
            continue
        if ln.startswith("Inmate Name") or ln.startswith("Identifier CID"):
            continue

        # 1) name + cid on same line
        m_nc = name_cid_pat.match(ln)
        if m_nc:
            current_name = m_nc.group("name").strip()
            current_cid = m_nc.group("cid").strip()
            last_key = None
            continue

        # 2) name alone line
        if looks_like_name(ln):
            current_name = ln.strip()
            current_cid = None
            last_key = None
            continue

        # 3) cid alone line (if name was previous line)
        m_cid = cid_alone_pat.match(ln)
        if m_cid and current_name:
            current_cid = m_cid.group("cid").strip()
            continue

        # If we don't have a name yet, can't attach anything
        if not current_name:
            continue

        # 4) date + booking + desc line (attach using current_name + current_cid)
        m_dbd = date_booking_desc_pat.match(ln)
        if m_dbd:
            date = m_dbd.group("date").strip()
            booking = m_dbd.group("booking").strip()
            desc = m_dbd.group("desc").strip()

            # Sometimes CID is embedded in the same line even if not at start
            if not current_cid:
                cid_search = re.search(r"\b(\d{6,7})\b", ln)
                if cid_search:
                    current_cid = cid_search.group(1)

            # If still no CID, we cannot match later — but we still store it with a synthetic CID key
            cid_value = current_cid if current_cid else f"NO_CID_{current_name}_{booking}"

            if desc and not is_probably_address(desc):
                key = (cid_value, booking)
                rec = records_by_key.get(key)
                if not rec:
                    rec = {
                        "cid": cid_value,
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

        # 5) wrapped charge line (all caps) after we’ve captured a record
        if last_key and ln.isupper() and len(ln) > 10 and not is_probably_address(ln):
            records_by_key[last_key]["charges"].append(ln)
            continue

    # finalize
    out = []
    for rec in records_by_key.values():
        # dedupe charges
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
# Parse: Bonds Issued Day 1
# -----------------------------
def parse_bonds_issued(lines: list[str]) -> list[dict]:
    records = []

    amt_pat = re.compile(r"\b([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})\b")
    cid_pat = re.compile(r"\b(\d{6,7})\b")
    date_pat = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
    bondno_pat = re.compile(r"^\b(\d{6,8})\b")
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
        name = clean_person_name(name_m.group(1))

        # offense: between name and last date
        name_pos_end = ln.find(name_m.group(1)) + len(name_m.group(1))
        last_date_pos = ln.rfind(mdate)
        offense = ln[name_pos_end:last_date_pos].strip(" -|")
        offense = re.sub(r"\s+", " ", offense).strip()
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

    # dedupe
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
def build_email(today_str: str, booked_day1: list[dict], bonds_day1: list[dict], booked_rolling: list[dict], rolling_days: int = 3) -> str:
    total_booked = len(booked_day1)

    charge_counts = Counter()
    for r in booked_day1:
        if r.get("top_charge"):
            charge_counts.update([r["top_charge"]])
    top_charges = [c for c, _ in charge_counts.most_common(3)]
    top_charges_text = ", ".join(top_charges) if top_charges else "N/A"

    total_bonds_set = len(bonds_day1)

    # match by CID
    rolling_by_cid = {}
    for r in booked_rolling:
        rolling_by_cid.setdefault(r["cid"], []).append(r)

    matched = []
    for b in bonds_day1:
        cid = b["cid"]
        if cid in rolling_by_cid:
            booking = rolling_by_cid[cid][0]
            matched.append({
                "name": booking["name"],
                "booking_no": booking.get("booking_no", ""),
                "book_in_date": booking.get("book_in_date", ""),
                "top_charge": booking.get("top_charge", ""),
                "bond_amount": b.get("amount", ""),
                "bond_mdate": b.get("mdate", "")
            })

    def table_row(cols):
        tds = "".join([f"<td style='padding:8px;border:1px solid #333;vertical-align:top'>{html_escape(str(c))}</td>" for c in cols])
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
      <p style="margin:0 0 6px 0"><b>{len(matched)}</b> bond records matched to someone booked-in within the last {rolling_days} days (matched by CID)</p>
      {matches_table}

      <p style="color:#666;margin-top:14px">
        Note: “Bonds Issued” reflects bond amounts <b>set</b> in the last 24 hours. It does not indicate bond payment, release, or custody status.
        Matching is done by CID across a rolling {rolling_days}-day window because bond setting often occurs after booking.
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
    debug = os.environ.get("DEBUG_PDF", "0") == "1"

    # Day 1 bookings
    booked_day1_pdf = fetch_pdf(BOOKED_IN_BASE + "01.PDF")
    booked_day1 = []
    if booked_day1_pdf:
        lines = extract_lines_from_pdf(booked_day1_pdf)
        if debug:
            debug_dump(lines, "BOOKED-IN DAY 1 RAW LINES")
        booked_day1 = parse_booked_in(lines, source_day="01")

        # If still zero, ALWAYS dump the first 80 lines (saves credits)
        if len(booked_day1) == 0:
            debug_dump(lines, "BOOKED-IN DAY 1 RAW LINES (AUTO DUMP BECAUSE ZERO BOOKINGS)")

    # Rolling bookings (01/02/03)
    booked_rolling = []
    for day in BOOKED_IN_DAYS:
        pdf_bytes = fetch_pdf(BOOKED_IN_BASE + day)
        if not pdf_bytes:
            continue
        lines = extract_lines_from_pdf(pdf_bytes)
        day_id = day.replace(".PDF", "")
        booked_rolling.extend(parse_booked_in(lines, source_day=day_id))

    # Bonds day 1
    bonds_day1 = []
    bonds_pdf = fetch_pdf(BONDS_DAY1_URL)
    if bonds_pdf:
        bonds_lines = extract_lines_from_pdf(bonds_pdf)
        bonds_day1 = parse_bonds_issued(bonds_lines)

    today_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Tarrant County Jail Report — {today_str}"
    html = build_email(today_str, booked_day1, bonds_day1, booked_rolling, rolling_days=3)
    send_email(subject, html)


if __name__ == "__main__":
    main()
