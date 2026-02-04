import ssl
import smtplib
from datetime import datetime, timedelta
from collections import Counter
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Reuse your existing, proven functions (NO changes to report.py)
from report import env, safe_int, fetch_pdf, parse_booked_in, DEFAULT_BOOKED_BASE_URL


# -----------------------------
# Charge categorization (simple, effective)
# -----------------------------
CATEGORY_RULES = [
    # Priority order matters
    ("DWI / Alcohol", [
        "DWI", "INTOX", "BAC", "DUI", "ALCH", "OPEN CONT", "PUBLIC INTOX",
        "DRUNK", "INTOXICATED"
    ]),
    ("Drugs / Possession", [
        "POSS", "CONTROLLED SUB", "CS", "MARIJ", "COCAINE", "METH", "HEROIN",
        "DRUG", "NARC", "PARAPH", "PG", "POSS CS"
    ]),
    ("Family Violence / Assault", [
        "FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "VIOLENCE",
        "INJURY", "CHOKE", "STRANG", "DOMESTIC"
    ]),
    ("Theft / Fraud", [
        "THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "CREDIT", "IDENTITY",
        "STOLEN", "SHOPLIFT"
    ]),
    ("Weapons", [
        "WEAPON", "FIREARM", "GUN", "CARRYING", "UCW", "UNL CARRYING"
    ]),
    ("Warrants / Court / Bond", [
        "WARRANT", "FTA", "FAIL TO APPEAR", "CONTEMPT", "BOND", "PAROLE", "PROB"
    ]),
    ("Evading / Resisting", [
        "EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"
    ]),
]

def normalize(s: str) -> str:
    return " ".join((s or "").strip().split())

def extract_city_fallback(rec: dict) -> str:
    """
    Fallback city extraction when r['city'] is missing.
    Attempts to parse city from address block inside description.
    """
    city = normalize(rec.get("city") or "")
    if city:
        return city

    text = (rec.get("description") or "").upper()

    # Common Tarrant County cities to look for
    KNOWN_CITIES = [
        "FORT WORTH", "ARLINGTON", "GRAND PRAIRIE",
        "MANSFIELD", "BURLESON", "EULESS",
        "BEDFORD", "HURST", "NORTH RICHLAND HILLS",
        "RICHLAND HILLS", "HALTOM CITY", "WATAUGA",
        "KELLER", "AZLE", "WHITE SETTLEMENT",
        "FOREST HILL", "BENBROOK", "CROWLEY",
        "EVERMAN", "SAGINAW", "LAKE WORTH",
        "WESTWORTH VILLAGE"
    ]

    for c in KNOWN_CITIES:
        if c in text:
            return c.title()

    return "Unknown"
    
def get_all_charge_text(rec: dict) -> str:
    # Your report.py stores all charges joined with \n in "description"
    return normalize((rec.get("description") or "")).upper()

def categorize_record(rec: dict) -> str:
    text = get_all_charge_text(rec)
    if not text:
        return "Other / Unknown"
    for category, needles in CATEGORY_RULES:
        for n in needles:
            if n in text:
                return category
    return "Other / Unknown"

def top_single_charge(booked_records: list[dict]) -> str:
    """
    Counts FIRST charge line per record (matches your existing logic idea),
    but returns the most common cleaned charge headline.
    """
    items = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first = desc.splitlines()[0].strip()
        first = normalize(first).upper()
        if first:
            items.append(first)
    if not items:
        return "Unknown"
    top = Counter(items).most_common(1)[0][0]
    return top.title()

def city_breakdown(booked_records: list[dict], top_n: int = 6) -> list[tuple[str, int]]:
    cities = []
    for r in booked_records:
        c = normalize(r.get("city") or "Unknown")
        if not c:
            c = "Unknown"
        cities.append(c)
    return Counter(cities).most_common(top_n)

def pct(part: int, whole: int) -> str:
    if whole <= 0:
        return "0%"
    return f"{round((part / whole) * 100)}%"

def format_snapshot(report_dt: datetime, booked_records: list[dict]) -> str:
    report_date_str = report_dt.strftime("%-m/%-d/%Y")
    arrests_date_str = (report_dt - timedelta(days=1)).strftime("%-m/%-d/%Y")

    total = len(booked_records)

    # Category % (your “60% DWI / 35% possession / 5% family violence” style)
    cat_counts = Counter()
    for r in booked_records:
        cat_counts[categorize_record(r)] += 1

    # We’ll surface the most relevant “headline” categories first
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

    top_charge = top_single_charge(booked_records)
    top_cities = city_breakdown(booked_records, top_n=6)

    # Build text email
    lines = []
    lines.append("UNCLASSIFIED // FOR INFORMATIONAL USE ONLY")
    lines.append(f"DAILY JAIL SNAPSHOT — TARRANT COUNTY, TX")
    lines.append("")
    lines.append(f"Report date:  {report_date_str}")
    lines.append(f"Arrests date: {arrests_date_str}")
    lines.append(f"Total bookings (last 24h): {total}")
    lines.append("")
    lines.append(f"Top charge today: {top_charge}")
    lines.append("")
    lines.append("Charge mix (share of bookings):")
    for k in preferred_order:
        v = cat_counts.get(k, 0)
        if v <= 0:
            continue
        lines.append(f"- {k}: {pct(v, total)} ({v})")
    lines.append("")

    lines.append("Arrests by city (top):")
    shown = 0
    city_total_shown = 0
    for city, count in top_cities:
        lines.append(f"- {city}: {pct(count, total)} ({count})")
        shown += 1
        city_total_shown += count

    other = total - city_total_shown
    if other > 0:
        lines.append(f"- Other: {pct(other, total)} ({other})")

    lines.append("")
    lines.append("Notes:")
    lines.append("- Stats are generated from CJ Reports booked-in data.")
    lines.append("- Use these numbers to make your daily FB graphic/post.")
    return "\n".join(lines)

def send_text_email(subject: str, body_text: str) -> None:
    """
    Uses same SMTP variables as your existing script.
    Sends to STAT_TO_EMAIL if set; otherwise TO_EMAIL.
    """
    to_email = env("STAT_TO_EMAIL", "").strip() or env("TO_EMAIL", "").strip()
    smtp_user = env("SMTP_USER", "").strip()
    smtp_pass = env("SMTP_PASS", "").strip()

    smtp_host = env("SMTP_HOST", "smtp.gmail.com").strip()
    smtp_port = safe_int(env("SMTP_PORT", "465"), 465)

    if not to_email or not smtp_user or not smtp_pass:
        raise RuntimeError("Missing required email env vars: TO_EMAIL(or STAT_TO_EMAIL), SMTP_USER, SMTP_PASS")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.attach(MIMEText(body_text, "plain", "utf-8"))

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_email], msg.as_string())

def main():
    booked_base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    booked_day = env("BOOKED_DAY", "01").strip()

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    subject = f"Daily Jail Snapshot — Tarrant County — {report_dt.strftime('%-m/%-d/%Y')}"
    body = format_snapshot(report_dt, booked_records)
    send_text_email(subject, body)

if __name__ == "__main__":
    main()
