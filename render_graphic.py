import os
from datetime import timedelta
from collections import Counter

# Reuse your proven functions from report.py (NO changes to report.py)
from report import env, fetch_pdf, parse_booked_in, DEFAULT_BOOKED_BASE_URL

from PIL import Image, ImageDraw, ImageFont


# -----------------------------
# Config
# -----------------------------
OUTPUT_LATEST = "output/daily-report-latest.png"
OUTPUT_ARCHIVE_DIR = "output/archive"

# Simple charge categorization
CATEGORY_RULES = [
    ("DWI / Alcohol", ["DWI", "INTOX", "BAC", "DUI", "ALCH", "OPEN CONT", "PUBLIC INTOX", "DRUNK", "INTOXICATED"]),
    ("Drugs / Possession", ["POSS", "CONTROLLED SUB", "CS", "MARIJ", "COCAINE", "METH", "HEROIN", "DRUG", "NARC", "PARAPH", "PG", "POSS CS"]),
    ("Family Violence / Assault", ["FAMILY", "FV", "ASSAULT", "AGG ASSAULT", "BODILY INJURY", "VIOLENCE", "INJURY", "CHOKE", "STRANG", "DOMESTIC"]),
    ("Theft / Fraud", ["THEFT", "BURGL", "ROBB", "FRAUD", "FORGERY", "CREDIT", "IDENTITY", "STOLEN", "SHOPLIFT"]),
    ("Weapons", ["WEAPON", "FIREARM", "GUN", "CARRYING", "UCW", "UNL CARRYING"]),
    ("Evading / Resisting", ["EVADING", "RESIST", "INTERFER", "OBSTRUCT", "FLEE"]),
    ("Warrants / Court / Bond", ["WARRANT", "FTA", "FAIL TO APPEAR", "CONTEMPT", "BOND", "PAROLE", "PROB"]),
]


def normalize(s: str) -> str:
    return " ".join((s or "").strip().split())


def categorize_record(rec: dict) -> str:
    text = normalize(rec.get("description") or "").upper()
    if not text:
        return "Other / Unknown"
    for category, needles in CATEGORY_RULES:
        for n in needles:
            if n in text:
                return category
    return "Other / Unknown"


def top_single_charge(booked_records: list[dict]) -> str:
    items = []
    for r in booked_records:
        desc = (r.get("description") or "").strip()
        if not desc:
            continue
        first = normalize(desc.splitlines()[0]).upper()
        if first:
            items.append(first)
    if not items:
        return "Unknown"
    top = Counter(items).most_common(1)[0][0]
    # Keep the original-ish casing (but readable)
    return top.title()


def pct_str(part: int, whole: int) -> str:
    if whole <= 0:
        return "0%"
    # Apple/editorial style: round to nearest whole percent
    return f"{round((part / whole) * 100)}%"


def compute_city(booked_records: list[dict], top_n: int = 12) -> tuple[list[tuple[str, int]], int]:
    cities = []
    for r in booked_records:
        c = normalize(r.get("city") or "Unknown")
        if not c:
            c = "Unknown"
        cities.append(c)
    counter = Counter(cities)
    top = counter.most_common(top_n)
    shown_total = sum(v for _, v in top)
    return top, shown_total


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """
    Uses DejaVu fonts (available on ubuntu-latest) to avoid bundling font files.
    """
    if bold:
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    else:
        path = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    return ImageFont.truetype(path, size)


def draw_round_rect(draw: ImageDraw.ImageDraw, xy, radius, fill, outline=None, width=1):
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=fill, outline=outline, width=width)


def draw_label(draw, x, y, text, font, fill):
    draw.text((x, y), text, font=font, fill=fill)


def render_png(report_dt, booked_records, county_label: str):
    # --- Theme (off-white, neutral, subtle dividers)
    BG = (248, 248, 246)
    INK = (17, 24, 39)
    MUTED = (100, 116, 139)
    DIV = (226, 232, 240)
    CHIP_BG = (236, 238, 241)
    PURPLE = (92, 60, 255)  # subtle brand accent you’ve been using

    # --- Canvas size (IG story friendly, also works everywhere)
    W, H = 1080, 1350
    pad = 72

    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)

    # Fonts
    f_top = load_font(34, bold=True)
    f_chip = load_font(28, bold=False)
    f_h1 = load_font(60, bold=True)
    f_meta_k = load_font(24, bold=True)
    f_meta_v = load_font(26, bold=False)
    f_h2 = load_font(34, bold=True)
    f_row = load_font(28, bold=False)
    f_row_b = load_font(28, bold=True)
    f_small = load_font(22, bold=False)
    f_small_b = load_font(22, bold=True)

    # Data
    arrests_dt = report_dt - timedelta(days=1)
    total = len(booked_records)

    # Charge mix
    cat_counts = Counter()
    for r in booked_records:
        cat_counts[categorize_record(r)] += 1

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

    # City
    top_cities, shown_total = compute_city(booked_records, top_n=12)
    other_city_count = max(total - shown_total, 0)

    # Top charge
    top_charge = top_single_charge(booked_records)

    # -----------------------------
    # Header bar (logo text + chip)
    # -----------------------------
    y = pad

    # Brand text (simple + clean). If you later want the actual logo image,
    # we can paste it in here too — but this is rock-solid for automation.
    draw_label(d, pad, y, "DAILYJAILREPORTS.COM", f_top, INK)

    # County chip on right
    chip_text = county_label
    chip_w = d.textlength(chip_text, font=f_chip) + 44
    chip_h = 52
    chip_x2 = W - pad
    chip_x1 = chip_x2 - chip_w
    chip_y1 = y - 6
    chip_y2 = chip_y1 + chip_h
    draw_round_rect(d, (chip_x1, chip_y1, chip_x2, chip_y2), radius=26, fill=CHIP_BG, outline=None)
    draw_label(d, chip_x1 + 22, chip_y1 + 10, chip_text, f_chip, INK)

    # Divider
    y = chip_y2 + 28
    d.line((pad, y, W - pad, y), fill=DIV, width=2)

    # -----------------------------
    # Headline
    # -----------------------------
    y += 34
    draw_label(d, pad, y, "Daily Jail Bookings", f_h1, INK)
    y += 78
    draw_label(d, pad, y, f"Top charge: {top_charge}", f_row, MUTED)
    y += 44

    # -----------------------------
    # Metadata row
    # -----------------------------
    meta_y = y + 20
    meta_h = 120
    draw_round_rect(d, (pad, meta_y, W - pad, meta_y + meta_h), radius=24, fill=(255, 255, 255), outline=DIV, width=2)

    col_w = (W - pad * 2) // 3
    meta_items = [
        ("REPORT DATE", report_dt.strftime("%-m/%-d/%Y")),
        ("ARREST DATE", arrests_dt.strftime("%-m/%-d/%Y")),
        ("TOTAL BOOKINGS", str(total)),
    ]
    for i, (k, v) in enumerate(meta_items):
        x = pad + i * col_w + 26
        draw_label(d, x, meta_y + 22, k, f_meta_k, MUTED)
        draw_label(d, x, meta_y + 56, v, f_meta_v, INK)

    y = meta_y + meta_h + 42

    # -----------------------------
    # Charge Mix table
    # -----------------------------
    draw_label(d, pad, y, "Charge Mix", f_h2, INK)
    y += 18
    d.line((pad, y + 40, W - pad, y + 40), fill=DIV, width=2)

    y += 60
    row_x_label = pad
    row_x_value = W - pad

    shown_rows = 0
    for k in preferred_order:
        v = cat_counts.get(k, 0)
        if v <= 0:
            continue
        left = k
        right = f"{pct_str(v, total)} ({v})"
        draw_label(d, row_x_label, y, left, f_row, INK)
        # right-aligned
        rw = d.textlength(right, font=f_row_b)
        draw_label(d, row_x_value - rw, y, right, f_row_b, INK)
        y += 44
        shown_rows += 1

    y += 12
    d.line((pad, y, W - pad, y), fill=DIV, width=2)
    y += 34

    # -----------------------------
    # Arrests by City table
    # -----------------------------
    draw_label(d, pad, y, "Arrests by City", f_h2, INK)
    y += 18
    d.line((pad, y + 40, W - pad, y + 40), fill=DIV, width=2)
    y += 60

    # Single-column list (fixes your “two columns identical” issue)
    for city, count in top_cities:
        left = city
        right = f"{pct_str(count, total)} ({count})"
        draw_label(d, row_x_label, y, left, f_row, INK)
        rw = d.textlength(right, font=f_row_b)
        draw_label(d, row_x_value - rw, y, right, f_row_b, INK)
        y += 44

    if other_city_count > 0:
        left = "All Other Cities"
        right = f"{pct_str(other_city_count, total)} ({other_city_count})"
        draw_label(d, row_x_label, y, left, f_row, INK)
        rw = d.textlength(right, font=f_row_b)
        draw_label(d, row_x_value - rw, y, right, f_row_b, INK)
        y += 44

    y += 10
    d.line((pad, y, W - pad, y), fill=DIV, width=2)
    y += 28

    # -----------------------------
    # Context band (center-aligned archival + statement)
    # -----------------------------
    band_h = 220
    draw_round_rect(d, (pad, y, W - pad, y + band_h), radius=24, fill=(255, 255, 255), outline=DIV, width=2)

    cx = W // 2
    band_y = y + 22

    # Archival stamp line (centered)
    arch = f"ARCHIVE • {report_dt.strftime('%Y-%m-%d')} • TARRANT COUNTY, TX"
    arch_w = d.textlength(arch, font=f_small_b)
    draw_label(d, cx - arch_w / 2, band_y, arch, f_small_b, PURPLE)

    band_y += 44
    title = "What this report represents"
    tw = d.textlength(title, font=f_small_b)
    draw_label(d, cx - tw / 2, band_y, title, f_small_b, INK)

    band_y += 34
    line1 = "Individuals booked into Tarrant County Jail during the stated 24-hour reporting window."
    lw1 = d.textlength(line1, font=f_small)
    draw_label(d, cx - lw1 / 2, band_y, line1, f_small, MUTED)

    band_y += 34
    line2 = "Charges shown reflect booking charges, not convictions."
    lw2 = d.textlength(line2, font=f_small)
    draw_label(d, cx - lw2 / 2, band_y, line2, f_small, MUTED)

    y = y + band_h + 26

    # -----------------------------
    # Footer disclaimer
    # -----------------------------
    foot = "Automated from public CJ data. Not affiliated with Tarrant County.  UNCLASSIFIED // INFORMATIONAL USE ONLY"
    fw = d.textlength(foot, font=f_small)
    draw_label(d, cx - fw / 2, H - 54, foot, f_small, MUTED)

    return img


def main():
    county_label = env("COUNTY_LABEL", "Tarrant County, TX").strip() or "Tarrant County, TX"

    booked_base = env("BOOKED_BASE_URL", DEFAULT_BOOKED_BASE_URL).rstrip("/")
    booked_day = env("BOOKED_DAY", "01").strip()

    booked_url = f"{booked_base}/{booked_day}.PDF"
    pdf_bytes = fetch_pdf(booked_url)

    report_dt, booked_records = parse_booked_in(pdf_bytes)

    os.makedirs(os.path.dirname(OUTPUT_LATEST), exist_ok=True)
    os.makedirs(OUTPUT_ARCHIVE_DIR, exist_ok=True)

    img = render_png(report_dt, booked_records, county_label)

    # Save latest
    img.save(OUTPUT_LATEST, format="PNG", optimize=True)

    # Save archive copy by report date
    archive_path = os.path.join(OUTPUT_ARCHIVE_DIR, f"{report_dt.strftime('%Y-%m-%d')}.png")
    img.save(archive_path, format="PNG", optimize=True)

    print(f"Saved: {OUTPUT_LATEST}")
    print(f"Saved: {archive_path}")


if __name__ == "__main__":
    main()
