import os
import io
import re
import requests
import pdfplumber
import pandas as pd
from collections import Counter
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

# Day 1 reports (current day)
BOOKED_IN_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF"
BONDS_URL = "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF"


# -----------------------------
# Helpers
# -----------------------------
def fetch_pdf(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def extract_lines_from_pdf(pdf_bytes: bytes) -> list[str]:
    lines: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = ln.strip()
                if ln:
                    lines.append(ln)
    return lines


def normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def looks_like_name(line: str) -> bool:
    # Typical format: LAST, FIRST MIDDLE (all caps in these PDFs)
    return bool(re.match(r"^[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?$", line))


def is_probably_address(line: str) -> bool:
    # Prevent address/city/zip lines from being treated as charges
    if " TX " in line:
        return True
    if re.search(r"\b\d{5}\b", line):  # ZIP
        return True
    # common street tokens
    street_tokens = [
        " ST", " AVE", " RD", " DR", " LN", " BLVD", " HWY", " PKWY",
        " CIR", " CT", " TRL", " PL", " TER", " WAY", " LOOP"
    ]
    if any(tok in line for tok in street_tokens) and re.search(r"\b\d+\b", line):
        return True
    return False


def clean_charge(line: str) -> str:
    return normalize_ws(line)


# -----------------------------
# Parse Booked-In (Day 1)
# -----------------------------
def parse_booked_in(lines: list[str]) -> pd.DataFrame:
    records = []
    current = None

    booking_pat = re.compile(r"\b(\d{2}-\d{7})\b")      # e.g., 26-0259182
    date_pat = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")

    # Often appears as: "26-0259182 DRIVING WHILE INTOXICATED 2ND"
    booking_charge_pat = re.compile(r"\b\d{2}-\d{7}\b\s+(.+)$")

    offense_keywords = [
        "ASSAULT", "DWI", "INTOX", "THEFT", "BURGLARY", "ROBBERY", "WARRANT",
        "POSS", "POSSESSION", "CONTROLLED", "MARIJUANA", "COCAINE", "METH",
        "FRAUD", "VIOL", "VIOLATION", "RESIST", "EVADING",
        "WEAPON", "FIREARM", "CRIMINAL", "TRESPASS", "HARASS",
        "KIDNAP", "SEX", "INDECENCY", "DISORDERLY", "PROBATION",
        "INJURY", "FAMILY", "CHILD", "ELDERLY"
    ]

    def flush():
        nonlocal current
        if not current:
            return

        # dedupe charges preserving order
        seen = set()
        charges = []
        for c in current["charges"]:
            if c and c not in seen:
                seen.add(c)
                charges.append(c)

        records.append({
            "name": current.get("name", ""),
            "booking_no": current.get("booking_no", ""),
            "book_in_date": current.get("book_in_date", ""),
            "charges": charges,
            "top_charge": charges[0] if charges else ""
        })
        current = None

    for raw in lines:
        ln = normalize_ws(raw)

        # Start a new person when we hit a name line
        if looks_like_name(ln):
            if current:
                flush()
            current = {"name": ln, "booking_no": "", "book_in_date": "", "charges": []}
            continue

        if not current:
            continue

        # Booking number
        if not current["booking_no"]:
            m = booking_pat.search(ln)
            if m:
                current["booking_no"] = m.group(1)

        # Date (best effort)
        if not current["book_in_date"]:
            m = date_pat.search(ln)
            if m:
                current["book_in_date"] = m.group(1)

        # Charges from booking + description lines
        m = booking_charge_pat.search(ln)
        if m:
            charge = clean_charge(m.group(1))
            if charge and not is_probably_address(charge):
                current["charges"].append(charge)
            continue

        # Fallback: accept ALL CAPS lines only if they look like charges and not addresses
        if ln.isupper() and len(ln) > 10 and not is_probably_address(ln):
            if any(k in ln for k in offense_keywords):
                current["charges"].append(clean_charge(ln))

    if current:
        flush()

    return pd.DataFrame(records).fillna("")


# -----------------------------
# Parse Bonds Issued (Day 1) — match by booking_no
# -----------------------------
def parse_bonds(lines: list[str]) -> pd.DataFrame:
    booking_pat = re.compile(r"\b(\d{2}-\d{7})\b")
    amt_pat = re.compile(r"\b([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})\b")

    recs = []
    for raw in lines:
        ln = normalize_ws(raw)
        b = booking_pat.search(ln)
        a = amt_pat.search(ln)
        if b and a:
            recs.append({
                "booking_no": b.group(1),
                "bond_amount": a.group(1)
            })

    df = pd.DataFrame(recs)
    if df.empty:
        return df

    # If multiple entries per booking_no, keep the highest amount (best practical signal)
    df["bond_amount_num"] = df["bond_amount"].str.replace(",", "", regex=False).astype(float)
    df = df.sort_values("bond_amount_num").groupby("booking_no", as_index=False).tail(1)
    df = df.drop(columns=["bond_amount_num"])
    return df


# -----------------------------
# Email
# -----------------------------
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


def build_email_html(today_str: str,
                     total_bookings: int,
                     top_charges: list[str],
                     new_bonds_set_count: int,
                     matched_bonds_count: int,
                     df: pd.DataFrame) -> str:
    rows = []
    for _, r in df.iterrows():
        name = r.get("name", "") or ""
        top_charge = r.get("top_charge", "") or "N/A"
        bond = (r.get("bond_amount", "") or "").strip() or "N/A"
        rows.append(
            f"<tr>"
            f"<td><b>{name}</b></td>"
            f"<td>{top_charge}</td>"
            f"<td><b>{bond}</b></td>"
            f"</tr>"
        )

    top_charges_text = ", ".join(top_charges) if top_charges else "N/A"

    html = f"""
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;line-height:1.4">
      <h2 style="margin:0 0 10px 0">Tarrant County Jail Report — {today_str}</h2>

      <p><b>{total_bookings}</b> new bookings in the last 24 hours</p>
      <p><b>Top charges:</b> {top_charges_text}</p>
      <p><b>New bonds set in the last 24 hours:</b> {new_bonds_set_count}</p>
      <p style="color:#666;margin-top:4px">
        (Bonds matched to today's bookings: {matched_bonds_count})
      </p>

      <hr style="margin:16px 0"/>

      <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;border:1px solid #ddd;width:100%">
        <tr style="background:#f6f6f6">
          <th align="left">Name</th>
          <th align="left">Top Charge</th>
          <th align="left">Bond Set (if any)</th>
        </tr>
        {''.join(rows)}
      </table>

      <p style="color:#666;margin-top:12px">
        Bond information reflects newly issued bond amounts only. It does not indicate bond payment, release, or custody status.
        Source: Tarrant County Day 1 Booked-In + Day 1 Bonds Issued reports.
      </p>
    </div>
    """
    return html


# -----------------------------
# Main
# -----------------------------
def main():
    # Fetch PDFs
    booked_pdf = fetch_pdf(BOOKED_IN_URL)
    bonds_pdf = fetch_pdf(BONDS_URL)

    # Extract text lines
    booked_lines = extract_lines_from_pdf(booked_pdf)
    bond_lines = extract_lines_from_pdf(bonds_pdf)

    # Parse
    booked_df = parse_booked_in(booked_lines)
    bonds_df = parse_bonds(bond_lines)

    # Count how many "new bonds set" appear in the Day 1 Bonds report (unique bookings)
    new_bonds_set_count = int(bonds_df["booking_no"].nunique()) if not bonds_df.empty else 0

    # Merge bonds onto booked-in via booking_no
    merged = booked_df.copy()
    if (not booked_df.empty) and (not bonds_df.empty) and ("booking_no" in booked_df.columns):
        merged = booked_df.merge(bonds_df, on="booking_no", how="left")
    else:
        merged["bond_amount"] = ""

    merged["bond_amount"] = merged["bond_amount"].fillna("")

    # How many of today's bookings have a bond amount matched?
    matched_bonds_count = int((merged["bond_amount"].astype(str).str.strip() != "").sum()) if not merged.empty else 0

    # Summary numbers
    total_bookings = len(merged)

    # Top charges
    charge_counts = Counter()
    for c in merged["top_charge"].tolist() if total_bookings else []:
        c = (c or "").strip()
        if c and not is_probably_address(c):
            charge_counts.update([c])

    top_charges = [c for c, _ in charge_counts.most_common(3)]

    # Email
    today_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Tarrant County Jail Report — {today_str}"
    html = build_email_html(
        today_str=today_str,
        total_bookings=total_bookings,
        top_charges=top_charges,
        new_bonds_set_count=new_bonds_set_count,
        matched_bonds_count=matched_bonds_count,
        df=merged
    )

    send_email(subject, html)


if __name__ == "__main__":
    main()
