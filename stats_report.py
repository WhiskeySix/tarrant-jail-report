import ssl
import smtplib
from datetime import datetime, timedelta
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Import proven logic from report.py (NO duplication)
from report import (
    env,
    safe_int,
    fetch_pdf,
    parse_booked_in,
    DEFAULT_BOOKED_BASE_URL,
)

# -----------------------------
# Charge categorization
# -----------------------------

CATEGORY_RULES = [
    ("DWI / Alcohol", [
        "DWI", "INTOX", "BAC", "DUI", "ALCOHOL", "DRUNK", "INTOXICATED",
        "PUBLIC INTOX", "OPEN CONT"
    ]),
    ("Drugs / Possession", [
        "POSS", "POSS CS", "CONTROLLED SUB", "CS", "DRUG", "NARC",
        "MARIJ", "METH", "COCAINE", "HEROIN", "PARAPH"
    ]),
    ("Family Violence / Assault", [
        "FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY",
        "CHOKE", "STRANG", "DOMESTIC"
    ]),
    ("Theft / Fraud", [
        "THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "IDENTITY",
        "STOLEN", "SHOPLIFT"
    ]),
    ("Weapons", [
        "WEAPON", "FIREARM", "GUN", "UCW", "UNL CARRYING"
    ]),
    ("Evading / Resisting", [
        "EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"
    ]),
    ("Warrants / Court / Bond", [
        "WARRANT", "FTA", "FAIL TO APPEAR", "BOND", "PAROLE", "PROBATION"
    ]),
]

# -----------------------------
# Helpers
# -----------------------------

def normalize(text: str) -> str:
    return " ".join((text or "").strip().split())

def pct(part: int, whole: int) -> str:
    if whole <= 0:
        return "0%"
    return f"{round((part / whole) * 100)}%"

def categorize_record(record: dict) -> str:
    text = normalize(record.get("description", "")).upper()
    if not text:
        return "Other / Unknown"

    for category, needles in CATEGORY_RULES:
        for n in needles:
            if n in text:
                return category

    return "Other / Unknown"

def top_single_charge(records: list[dict]) -> str:
    charges = []
    for r in records:
        desc = (r.get("description") or "").strip()
        if desc:
            charges.append(desc.splitlines()[0].upper())

    if not charges:
        return "Unknown"

    return Counter(charges).most_common(1)[0][0].title()

def city_breakdown(records: list[dict], top_n: int = 12):
    cities = []
    for r in records:
        city = normalize(r.get("city", ""))
        if city:
            cities.append(city)
    return Counter(cities).most_common(top_n)

# -----------------------------
# Snapshot formatter
# -----------------------------

def format_snapshot(report_dt: datetime, records: list[dict]) -> str:
    report_date = report_dt.strftime("%-m/%-d/%Y")
    arrests_date = (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y")
    total = len(records)

    # Charge mix
    cat_counts = Counter(categorize_record(r) for r in records)

    preferred_order = [
        "DWI / Alcohol",
        "Drugs / Possession",
        "Family Violence / Assault",
        "Theft / Fraud",
        "Weapons",
        "Evading / Resisting",
        "Warrants / Court / Bond",
        "Other / Unknown",
    ]

    top_charge = top_single_charge(records)
    top_cities = city_breakdown(records, top_n=12)

    lines = []
    lines.append("UNCLASSIFIED // FOR INFORMATIONAL USE ONLY")
    lines.append("DAILY JAIL SNAPSHOT — TARRANT COUNTY, TX")
    lines.append("")
    lines.append(f"Report date:  {report_date}")
    lines.append(f"Arrests date: {arrests_date}")
    lines.append(f"Total bookings (last 24h): {total}")
    lines.append("")
    lines.append(f"Top charge today: {top_charge}")
    lines.append("")
    lines.append("Charge mix (share of bookings):")

    for cat in preferred_order:
        count = cat_counts.get(cat, 0)
        if count:
            lines.append(f"- {cat}: {pct(count, total)} ({count})")

    lines.append("")
    lines.append("Arrests by city (top):")

    shown_total = 0
    for city, count in top_cities:
        lines.append(f"- {city}: {pct(count, total)} ({count})")
        shown_total += count

    remaining = total - shown_total
    if remaining > 0:
        lines.append(f"- All Other Cities: {pct(remaining, total)} ({remaining})")

    lines.append("")
    lines.append("Notes:")
    lines.append("- Stats generated from Tarrant County CJ Reports booked-in data.")
    lines.append("- Intended for daily social + visual reporting.")

    return "\n".join(lines)

# -----------------------------
# Email sender
# -----------------------------

def send_email(subject: str, body: str):
    to_email = env("STAT_TO_EMAIL") or env("TO_EMAIL")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    smtp_host = env("SMTP_HOST", "smtp.gmail.com")
    smtp_port = safe_int(env("SMTP_PORT", "465"), 465)

    if not to_email or not smtp_user or not smtp_pass:
        raise RuntimeError("Missing required SMTP env vars")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.attach(MIMEText(body, "plain", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_email], msg.as_string())

# -----------------------------
# Main
# -----------------------------

def main():
    base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    day = env("BOOKED_DAY", "01").strip()
    url = f"{base}/{day}.PDF"

    pdf = fetch_pdf(url)
    report_dt, records = parse_booked_in(pdf)

    subject = f"Daily Jail Snapshot — Tarrant County — {report_dt.strftime('%-m/%-d/%Y')}"
    body = format_snapshot(report_dt, records)
    send_email(subject, body)

if __name__ == "__main__":
    main()
