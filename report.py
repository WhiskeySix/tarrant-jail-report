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


def is_probably_address(line: str) -> bool:
    # Addresses / cities / zips that kept showing up as "charges"
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


def looks_like_name(line: str) -> bool:
    # Typical format: LAST, FIRST MIDDLE
    # Keep it strict to avoid false positives
    return bool(re.match(r"^[A-Z][A-Z' -]+,\s*[A-Z][A-Z' -]+(?:\s+[A-Z][A-Z' -]+)?$", line))


def clean_charge(line: str) -> str:
    # Strip obvious noise
    line = normalize_ws(line)
    line = line.replace("  ", " ")
    return line


# -----------------------------
# Parse Booked-In
# -----------------------------
def parse_booked_in(lines: list[str]) -> pd.DataFrame:
    records = []
    current = None

    booking_pat = re.compile(r"\b(\d{2}-\d{7})\b")      # e.g. 26-0259182
    cid_pat = re.compile(r"\b(\d{6,7})\b")              # typical CID length
    date_pat = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")

    # Charges in the PDF often appear on lines like:
    # "26-0259182 DRIVING WHILE INTOXICATED"
    booking_charge_pat = re.compile(r"\b\d{2}-\d{7}\b\s+(.+)$")

    def flush():
        nonlocal current
        if not current:
            return
        # dedupe charges preserving order
        seen = set()
        charges = []
        for c in current["charges"]:
            if c not in seen:
                seen.add(c)
                charges.append(c)

        records.append({
            "name": current.get("name", ""),
            "cid": current.get("cid", ""),
            "booking_no": current.get("booking_no", ""),
            "book_in_date": current.get("book_in_date", ""),
            "charges": charges,
            "top_charge": charges[0] if charges else ""
        })
        current = None

    for ln in lines:
        ln = normalize_ws(ln)

        # Start a new person block on a Name line
        if looks_like_name(ln):
            if current:
                flush()
            current = {"name": ln, "cid": "", "booking_no": "", "book_in_date": "", "charges": []}
            continue

        if not current:
            continue

        # Capture CID (best-effort)
        if not current["cid"]:
            m = cid_pat.search(ln)
            if m:
                current["cid"] = m.group(1)

        # Capture booking number (best-effort)
        if not current["booking_no"]:
            m = booking_pat.search(ln)
            if m:
                current["booking_no"] = m.group(1)

        # Capture date (best-effort)
        if not current["book_in_date"]:
            m = date_pat.search(ln)
            if m:
                current["book_in_date"] = m.group(1)

        # Capture charges from "booking + charge description" lines
        m = booking_charge_pat.search(ln)
        if m:
            charge = clean_charge(m.group(1))
            if charge and not is_probably_address(charge):
                current["charges"].append(charge)
            continue

        # Sometimes charges appear as all-caps lines without booking number.
        # We'll only accept those if they contain common offense terms and are NOT addresses.
        if ln.isupper() and len(ln) > 10 and not is_probably_address(ln):
            offense_keywords = [
                "ASSAULT", "DWI", "INTOX", "THEFT", "BURGLARY", "ROBBERY", "WARRANT",
                "POSS", "POSSESSION", "CONTROLLED", "MARIJUANA", "COCAINE", "METH",
                "FRAUD", "VIOL", "VIOLATION", "RESIST", "EVADING",
                "WEAPON", "FIREARM", "CRIMINAL", "TRESPASS", "HARASS",
                "KIDNAP", "SEX", "INDECENCY", "DISORDERLY", "PROBATION"
            ]
            if any(k in ln for k in offense_keywords):
                current["charges"].append(clean_charge(ln))

    # flush last
    if current:
        flush()

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Clean empties
    df = df.fillna("")
    return df


# -----------------------------
# Parse Bonds
# -----------------------------
def parse_bonds(lines: list[str]) -> pd.DataFrame:
    # Bonds PDF can be inconsistent; we’ll capture CID and dollar amount.
    cid_pat = re.compile(r"\b(\d{6,7})\b")
    amt_pat = re.compile(r"\b([0-9]{1,3}(?:,[0-9]{3})*\.[0-9]{2})\b")

    recs = []
    for ln in lines:
        ln = normalize_ws(ln)
        cid = cid_pat.search(ln)
        amt = amt_pat.search(ln)
        if cid and amt:
            recs.append({
                "cid": cid.group(1),
                "bond_amount": amt.group(1)
            })

    df = pd.DataFrame(recs)
    if df.empty:
        return df

    # If multiple bonds per CID, keep the max (best practical signal)
    df["bond_amount_num"] = df["bond_amount"].str.replace(",", "", regex=False).astype(float)
    df = df.sort_values("bond_amount_num").groupby("cid", as_index=False).tail(1)
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


def build_email_html(today_str: str, total: int, top_charges: list[str], bonds_issued: str, df: pd.DataFrame) -> str:
    # Show a clean table (Name, Top Charge, Bond) but keep full data in the email (optional)
    rows = []
    for _, r in df.iterrows():
        name = r.get("name", "")
        top_charge = r.get("top_charge", "") or "N/A"
        bond = r.get("bond_amount", "") or "N/A"
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

      <p><b>{total}</b> new bookings in the last 24 hours</p>
      <p><b>Top charges:</b> {top_charges_text}</p>
      <p><b>Bonds issued:</b> {bonds_issued}</p>

      <hr style="margin:16px 0"/>

      <table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;border:1px solid #ddd;width:100%">
        <tr style="background:#f6f6f6">
          <th align="left">Name</th>
          <th align="left">Top Charge</th>
          <th align="left">Bond</th>
        </tr>
        {''.join(rows)}
      </table>

      <p style="color:#666;margin-top:12px">
        Informational use only. Source: Tarrant County daily booked-in and bond reports.
      </p>
    </div>
    """
    return html


# -----------------------------
# Main
# -----------------------------
def main():
    booked_pdf = fetch_pdf(BOOKED_IN_URL)
    bonds_pdf = fetch_pdf(BONDS_URL)

    booked_lines = extract_lines_from_pdf(booked_pdf)
    bond_lines = extract_lines_from_pdf(bonds_pdf)

    booked_df = parse_booked_in(booked_lines)
    bonds_df = parse_bonds(bond_lines)

    # Merge bonds onto booked-in using CID (best available key across both)
    merged = booked_df.copy()
    if not booked_df.empty and not bonds_df.empty and "cid" in booked_df.columns:
        merged = booked_df.merge(bonds_df, on="cid", how="left")
    else:
        merged["bond_amount"] = ""

    merged["bond_amount"] = merged["bond_amount"].fillna("")
    total = len(merged)

    # Determine top charges by counting "top_charge"
    charge_counts = Counter()
    for c in merged["top_charge"].tolist() if total else []:
        if c and not is_probably_address(c):
            charge_counts.update([c])

    top_charges = [c for c, _ in charge_counts.most_common(3)]
    bonds_issued = "Yes" if (merged["bond_amount"].astype(str).str.strip() != "").any() else "No"

    today_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Tarrant County Jail Report — {today_str}"
    html = build_email_html(today_str, total, top_charges, bonds_issued, merged)

    send_email(subject, html)


if __name__ == "__main__":
    main()
