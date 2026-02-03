import os
import io
import re
import requests
import pdfplumber
import pandas as pd
from datetime import date
from collections import Counter
import smtplib
from email.mime.text import MIMEText

BOOKED_IN_URL = "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF"
BONDS_URL = "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF"

def fetch_pdf(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def extract_lines(pdf_bytes):
    lines = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            for line in text.split("\n"):
                line = line.strip()
                if line:
                    lines.append(line)
    return lines

def parse_booked(lines):
    records = []
    current = None

    for line in lines:
        if re.match(r"^[A-Z' -]+,\s+[A-Z' -]+$", line):
            if current:
                records.append(current)
            current = {
                "name": line,
                "charges": [],
                "booking": "",
                "date": ""
            }
        elif current:
            if re.search(r"\d{2}-\d{7}", line):
                current["booking"] = re.search(r"\d{2}-\d{7}", line).group()
            elif re.search(r"\d{1,2}/\d{1,2}/\d{4}", line) and not current["date"]:
                current["date"] = re.search(r"\d{1,2}/\d{1,2}/\d{4}", line).group()
            elif len(line) > 6 and line.isupper():
    # Filter out address/location lines that are often ALL CAPS in the PDF
    address_tokens = [" ST", " AVE", " RD", " DR", " LN", " BLVD", " HWY", " PKWY", " CIR", " CT", " TRL", " PL", " TER"]
    if (" TX " in line) or re.search(r"\b\d{5}\b", line) or any(tok in line for tok in address_tokens):
        continue

    # Keep lines that look like criminal charges (keywords-based heuristic)
    charge_keywords = [
        "ASSAULT", "DWI", "INTOX", "THEFT", "BURGLARY", "ROBBERY", "WARRANT",
        "POSS", "POSSESSION", "CONTROLLED", "MARIJUANA", "COCAINE", "METH",
        "FRAUD", "FAMILY", "VIOL", "VIOLATION", "RESIST", "EVADING",
        "WEAPON", "FIREARM", "CRIMINAL", "TRESPASS", "HARASS", "KIDNAP",
        "SEX", "INDECENCY", "PUBLIC", "DISORDERLY", "FAILURE", "PROBATION"
    ]
    if any(k in line for k in charge_keywords):
        current["charges"].append(line)


    if current:
        records.append(current)

    return pd.DataFrame(records)

def parse_bonds(lines):
    bonds = []
    for line in lines:
        m = re.search(r"(\d{6,7}).*?([0-9,]+\.\d{2})", line)
        if m:
            bonds.append({
                "cid": m.group(1),
                "bond": m.group(2)
            })
    return pd.DataFrame(bonds)

def send_email(subject, body):
    msg = MIMEText(body, "html")
    msg["Subject"] = subject
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = os.environ["TO_EMAIL"]

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
        server.send_message(msg)

def main():
    booked_pdf = fetch_pdf(BOOKED_IN_URL)
    bond_pdf = fetch_pdf(BONDS_URL)

    booked_lines = extract_lines(booked_pdf)
    bond_lines = extract_lines(bond_pdf)

    booked_df = parse_booked(booked_lines)
    bond_df = parse_bonds(bond_lines)

    merged = booked_df.merge(bond_df, how="left", left_index=True, right_index=True)
    merged["bond"] = merged["bond"].fillna("N/A")

    charge_counts = Counter()
    for charges in merged["charges"]:
        if charges:
            charge_counts.update(charges)

    top_charges = ", ".join([c for c, _ in charge_counts.most_common(3)])
    total = len(merged)
    bonds_issued = "Yes" if bond_df.shape[0] > 0 else "No"

    today = date.today().strftime("%b %d, %Y")

    html = f"""
    <h2>Tarrant County Jail Report — {today}</h2>
    <p><strong>{total}</strong> new bookings in the last 24 hours</p>
    <p><strong>Top charges:</strong> {top_charges}</p>
    <p><strong>Bonds issued:</strong> {bonds_issued}</p>
    <hr>
    <table border="1" cellpadding="6" cellspacing="0">
        <tr>
            <th>Name</th>
            <th>Top Charge</th>
            <th>Bond</th>
        </tr>
    """

    for _, row in merged.iterrows():
        charge = row["charges"][0] if row["charges"] else "N/A"
        html += f"""
        <tr>
            <td><b>{row['name']}</b></td>
            <td>{charge}</td>
            <td>{row['bond']}</td>
        </tr>
        """

    html += "</table>"

    subject = f"Tarrant County Jail Report — {today}"
    send_email(subject, html)

if __name__ == "__main__":
    main()
