#!/usr/bin/env python3
"""
Tarrant County Jail Report (Email HTML)

Outputs a single HTML email with:
1) New Bookings (last 24 hours) from Booked-In Day 1 (01.PDF)
2) New Bonds Set (last 24 hours) from Bonds Day 1 (01.PDF)
3) Rolling Match: Bonds Set vs Booked-In last 3 days (CID match)

Config (env vars optional):
- BOOKED_BASE_URL  default: https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF
- BOOKED_DAYS      default: 3   (downloads 01..N PDFs)
- BONDS_PDF_URL     optional exact URL to bonds day1 PDF
- BONDS_BASE_URL    optional base url to try if BONDS_PDF_URL not set
- OUTPUT_HTML       default: report.html

Notes:
- "Bonds Set" reflects bond amounts SET/ISSUED in the last 24 hours per the report.
  It does NOT mean paid, released, or custody status.
"""

import os
import re
import sys
import io
import html
import math
import datetime as dt
from collections import Counter, defaultdict

import requests
import pdfplumber


# ---------------------------
# Utilities
# ---------------------------

def eprint(*args):
    print(*args, file=sys.stderr)

def money_clean(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    # normalize "1,000.00" style
    m = re.search(r"\d{1,3}(?:,\d{3})*(?:\.\d{2})", s)
    return m.group(0) if m else s

def safe(s: str) -> str:
    return html.escape(s or "")

def most_common_charges(charge_list, n=3):
    c = Counter([c for c in charge_list if c and c.upper() != "N/A"])
    return [x for x, _ in c.most_common(n)]

def fetch_pdf_bytes(url: str, timeout=30) -> bytes:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.content

def try_urls(urls):
    for u in urls:
        try:
            r = requests.get(u, timeout=20)
            if r.status_code == 200 and r.headers.get("content-type", "").lower().startswith("application/pdf"):
                return u
            # sometimes pdf content-type is missing; still accept if bytes look like PDF
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                return u
        except Exception:
            continue
    return None

def pdf_to_text_lines(pdf_bytes: bytes):
    lines = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for ln in text.splitlines():
                ln = ln.rstrip()
                if ln.strip():
                    lines.append(ln)
    return lines


# ---------------------------
# BOOKED-IN parsing
# ---------------------------

BOOKED_HEADER_RE = re.compile(r"Inmates Booked In During the Past 24 Hours", re.I)
BOOKED_COL_RE = re.compile(r"Inmate Name\s+Identifier CID\s+Book In Date\s+Booking No\.\s+Description", re.I)

CID_RE = re.compile(r"\b(\d{6,7})\b")              # CID is 6-7 digits in your sample
BOOKDATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
BOOKNO_RE = re.compile(r"\b(\d{2}-\d{7})\b")       # 26-0259185 format
ALLCAPS_LINE_RE = re.compile(r"^[A-Z0-9 ,.';()/-]+$")

def parse_booked_in(pdf_bytes: bytes):
    """
    Returns list of dicts:
    {
      name, cid, book_in_date, booking_no, charges(list[str]), top_charge
    }

    This parser follows the actual PDF pattern:
    - A person row starts with LAST, FIRST...
    - Address lines follow (we ignore)
    - Then a line containing CID + BookInDate
    - Then one or more lines containing BookingNo + Description (sometimes multiple booking lines)
    """
    lines = pdf_to_text_lines(pdf_bytes)

    # strip obvious header rows and page markers
    cleaned = []
    for ln in lines:
        if BOOKED_HEADER_RE.search(ln):
            continue
        if re.search(r"Report Date:", ln):
            continue
        if re.search(r"Page:\s+\d+\s+of\s+\d+", ln):
            continue
        if BOOKED_COL_RE.search(ln):
            continue
        cleaned.append(ln)

    records = []
    i = 0
    current = None

    def flush_current():
        nonlocal current
        if not current:
            return
        # derive top charge
        charges = current.get("charges", [])
        current["top_charge"] = charges[0] if charges else "N/A"
        records.append(current)
        current = None

    while i < len(cleaned):
        ln = cleaned[i].strip()

        # New person starts with something like "ARRIAGA, JESSIE"
        # In the text, it's all-caps and has a comma
        if "," in ln and ALLCAPS_LINE_RE.match(ln) and not BOOKNO_RE.search(ln) and not CID_RE.search(ln):
            # if we were building a person, flush before starting new
            flush_current()
            current = {
                "name": ln,
                "cid": "",
                "book_in_date": "",
                "booking_no": "",
                "charges": []
            }
            i += 1
            continue

        # If we have a current person, look for CID + date line
        if current:
            # line containing CID and date
            cid_m = CID_RE.search(ln)
            date_m = BOOKDATE_RE.search(ln)

            # Many address lines do not contain CID/date, ignore them
            if cid_m and date_m and not current["cid"]:
                current["cid"] = cid_m.group(1)
                current["book_in_date"] = date_m.group(1)
                i += 1
                continue

            # booking number + description lines
            # These can repeat multiple times for same person
            bookno_m = BOOKNO_RE.search(ln)
            if bookno_m:
                bno = bookno_m.group(1)
                # description is remainder after booking number
                desc = ln.split(bno, 1)[-1].strip()
                desc = desc if desc else "N/A"

                # set booking_no once (keep first)
                if not current["booking_no"]:
                    current["booking_no"] = bno

                # keep each description as a charge item
                if desc and desc.upper() != "N/A":
                    current["charges"].append(desc)
                i += 1
                continue

        i += 1

    flush_current()

    # sanity filter: keep only records with CID + booking no
    records = [r for r in records if r.get("cid") and r.get("booking_no")]

    return records

def parse_report_date_from_booked(pdf_bytes: bytes):
    # Get "Report Date: 2/2/2026" from first page text
    lines = pdf_to_text_lines(pdf_bytes)
    for ln in lines[:50]:
        m = re.search(r"Report Date:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", ln)
        if m:
            return m.group(1)
    return ""


# ---------------------------
# BONDS parsing
# ---------------------------

def parse_bonds(pdf_bytes: bytes):
    """
    Attempts to parse Bonds Issued/Set report.

    Expected fields (best-effort):
    {
      name, cid, offense, bond_set, mdate
    }

    Uses regex scanning across extracted lines. The bonds PDF tends to be table-like with:
    - CID column present
    - Name like "BOSLEY, ADAM"
    - Offense text in caps
    - Amount like 1,000.00
    - MDate like 2/1/2026
    """
    lines = pdf_to_text_lines(pdf_bytes)

    cleaned = []
    for ln in lines:
        if re.search(r"List of Bonds Issued Over the last 24 Hours", ln, re.I):
            continue
        if re.search(r"Page:\s+\d+\s+of\s+\d+", ln):
            continue
        # remove column header lines
        if re.search(r"\bBond Number\b", ln) and re.search(r"\bAmount\b", ln) and re.search(r"\bCID\b", ln):
            continue
        cleaned.append(ln.strip())

    recs = []
    # We’ll detect records by finding NAME + CID + amount + mdate within the next few lines.
    # Bonds tables often wrap, so we use a sliding window.
    for idx, ln in enumerate(cleaned):
        # Candidate line must contain a name
        if "," not in ln or not ALLCAPS_LINE_RE.match(ln):
            continue

        name = ln.strip()
        window = " | ".join(cleaned[idx: idx+6])

        cid_m = CID_RE.search(window)
        amt_m = re.search(r"\b\d{1,3}(?:,\d{3})*(?:\.\d{2})\b", window)
        mdate_m = re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", window)

        if not (cid_m and amt_m and mdate_m):
            continue

        cid = cid_m.group(1)
        bond_set = amt_m.group(0)
        mdate = mdate_m.group(0)

        # offense: try to grab caps words between name and amount, excluding obvious noise
        # We'll search for the longest ALLCAPS phrase containing letters.
        offense = "N/A"
        offense_candidates = []
        for j in range(idx+1, min(idx+6, len(cleaned))):
            l2 = cleaned[j]
            if not l2:
                continue
            if re.search(r"\bSurety\b", l2, re.I):
                continue
            if CID_RE.search(l2):
                continue
            if re.search(r"\b\d{1,3}(?:,\d{3})*(?:\.\d{2})\b", l2):
                continue
            if re.search(r"\b\d{1,2}/\d{1,2}/\d{4}\b", l2):
                continue
            if ALLCAPS_LINE_RE.match(l2) and re.search(r"[A-Z]{3,}", l2):
                offense_candidates.append(l2.strip())

        # choose best candidate: longest string with letters
        if offense_candidates:
            offense = max(offense_candidates, key=lambda s: len(re.sub(r"[^A-Z]", "", s)))

        recs.append({
            "name": name,
            "cid": cid,
            "offense": offense,
            "bond_set": bond_set,
            "mdate": mdate
        })

    # de-dupe (same CID+amount+mdate+name)
    seen = set()
    uniq = []
    for r in recs:
        k = (r["cid"], r["bond_set"], r["mdate"], r["name"])
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)

    return uniq


# ---------------------------
# HTML rendering
# ---------------------------

def render_table(headers, rows):
    th = "".join(f"<th>{safe(h)}</th>" for h in headers)
    out = ["""
    <table style="width:100%;border-collapse:collapse;margin:12px 0;font-family:Arial,sans-serif;">
      <thead>
        <tr style="background:#1f2937;color:#fff;">
          %s
        </tr>
      </thead>
      <tbody>
    """ % th]

    for r in rows:
        tds = "".join(f"<td style='border:1px solid #374151;padding:10px;vertical-align:top;'>{safe(str(c))}</td>" for c in r)
        out.append(f"<tr style='background:#0b1220;color:#e5e7eb;'>{tds}</tr>")
    out.append("</tbody></table>")
    return "".join(out)

def build_html(title, report_date, booked_today, bonds_today, matches, booked_days):
    # Charges summary
    all_top = [r.get("top_charge", "N/A") for r in booked_today]
    top_charges = most_common_charges(all_top, n=3)
    top_charges_txt = ", ".join(top_charges) if top_charges else "N/A"

    # Section 1 rows
    sec1_rows = []
    for r in booked_today[:50]:
        sec1_rows.append([r["name"], r["booking_no"], r.get("top_charge","N/A")])

    # Section 2 rows
    sec2_rows = []
    for b in bonds_today[:50]:
        sec2_rows.append([b["name"], b.get("offense","N/A"), b.get("bond_set",""), b.get("mdate","")])

    # Section 3 rows
    sec3_rows = []
    for m in matches[:75]:
        sec3_rows.append([
            m["name"],
            m["booking_no"],
            m["book_in_date"],
            m["top_charge"],
            m["bond_set"],
            m["mdate"],
        ])

    booked_days_note = f"{booked_days} days" if booked_days else "3 days"

    html_out = f"""
    <div style="max-width:900px;margin:0 auto;background:#0b1220;color:#e5e7eb;padding:28px;border-radius:12px;font-family:Arial,sans-serif;">
      <h1 style="margin:0 0 8px 0;font-size:34px;letter-spacing:0.2px;">{safe(title)} — {safe(report_date or "")}</h1>

      <div style="margin:18px 0 10px 0;padding:14px;background:#111827;border:1px solid #374151;border-radius:10px;">
        <div style="font-size:18px;font-weight:700;">1) New Bookings (Last 24 Hours)</div>
        <div style="margin-top:8px;font-size:16px;"><b>{len(booked_today)}</b> new bookings</div>
        <div style="margin-top:6px;font-size:16px;"><b>Top charges:</b> {safe(top_charges_txt)}</div>
      </div>
      {render_table(["Name","Booking No","Top Charge"], sec1_rows) if sec1_rows else "<div style='padding:12px;background:#111827;border:1px solid #374151;border-radius:10px;'>No bookings parsed from Booked-In Day 1. If this is wrong, the parser didn’t find CID/BookingNo rows.</div>"}

      <div style="margin:22px 0 10px 0;padding:14px;background:#111827;border:1px solid #374151;border-radius:10px;">
        <div style="font-size:18px;font-weight:700;">2) New Bonds Set (Last 24 Hours)</div>
        <div style="margin-top:8px;font-size:16px;"><b>{len(bonds_today)}</b> bonds set</div>
      </div>
      {render_table(["Name","Offense (Bonds Report)","Bond Set","MDate"], sec2_rows) if sec2_rows else "<div style='padding:12px;background:#111827;border:1px solid #374151;border-radius:10px;'>No bonds parsed. This usually means the bonds PDF URL is wrong or the format changed.</div>"}

      <div style="margin:22px 0 10px 0;padding:14px;background:#111827;border:1px solid #374151;border-radius:10px;">
        <div style="font-size:18px;font-weight:700;">3) Rolling Match (Bonds Set vs. Booked-In Last {safe(booked_days_note)})</div>
        <div style="margin-top:8px;font-size:16px;">
          <b>{len(matches)}</b> bond records matched to someone booked-in within the last {safe(booked_days_note)} (matched by CID)
        </div>
      </div>
      {render_table(["Name","Booking No","Book In Date","Top Charge","Bond Set","MDate"], sec3_rows) if sec3_rows else "<div style='padding:12px;background:#111827;border:1px solid #374151;border-radius:10px;'>No matches found. If you expect matches, it means CID extraction failed in either Booked-In PDFs or Bonds PDF.</div>"}

      <div style="margin-top:18px;color:#9ca3af;font-size:14px;line-height:1.4;">
        <b>Note:</b> The “Bonds Issued/Set” report reflects bond amounts <b>set</b> in the last 24 hours. It does <u>not</u> indicate bond payment, release, or custody status.
        Matching is done by <b>CID</b> across a rolling {safe(booked_days_note)} window because bond setting can occur after booking.
      </div>
    </div>
    """
    return html_out


# ---------------------------
# Main
# ---------------------------

def main():
    booked_base = os.getenv("BOOKED_BASE_URL", "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF").rstrip("/")
    booked_days = int(os.getenv("BOOKED_DAYS", "3"))
    output_html = os.getenv("OUTPUT_HTML", "report.html")

    # Download booked-in PDFs Day 1..N
    booked_pdfs = []
    for d in range(1, booked_days + 1):
        url = f"{booked_base}/{d:02d}.PDF"
        eprint(f"[booked-in] downloading: {url}")
        booked_pdfs.append((d, url, fetch_pdf_bytes(url)))

    # Day 1 report date (for title)
    report_date = parse_report_date_from_booked(booked_pdfs[0][2])

    # Parse booked records for each day
    booked_by_day = {}
    for d, url, bts in booked_pdfs:
        recs = parse_booked_in(bts)
        booked_by_day[d] = recs
        eprint(f"[booked-in] day {d:02d} parsed records: {len(recs)}")

    booked_today = booked_by_day.get(1, [])
    booked_last_n = []
    for d in range(1, booked_days + 1):
        booked_last_n.extend(booked_by_day.get(d, []))

    # Index booked-in by CID for matching (keep first occurrence, but also preserve booking no/date)
    booked_by_cid = {}
    for r in booked_last_n:
        cid = r.get("cid")
        if cid and cid not in booked_by_cid:
            booked_by_cid[cid] = r

    # Bonds PDF URL resolution
    bonds_pdf_url = os.getenv("BONDS_PDF_URL", "").strip()
    if not bonds_pdf_url:
        # Try common bases/paths (you can override with BONDS_PDF_URL once you know it)
        # If you know the correct endpoint, set BONDS_PDF_URL and you're done.
        candidates = []
        base_override = os.getenv("BONDS_BASE_URL", "").strip().rstrip("/")
        if base_override:
            candidates.append(f"{base_override}/01.PDF")

        # common guesses
        candidates.extend([
            "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF",
            "https://cjreports.tarrantcounty.com/Reports/BondIssued/FinalPDF/01.PDF",
            "https://cjreports.tarrantcounty.com/Reports/JailBonds/FinalPDF/01.PDF",
            "https://cjreports.tarrantcounty.com/Reports/Bonds/FinalPDF/01.PDF",
            "https://cjreports.tarrantcounty.com/Reports/JailBond/FinalPDF/01.PDF",
        ])

        bonds_pdf_url = try_urls(candidates) or ""

    bonds_today = []
    if bonds_pdf_url:
        eprint(f"[bonds] downloading: {bonds_pdf_url}")
        bonds_bytes = fetch_pdf_bytes(bonds_pdf_url)
        bonds_today = parse_bonds(bonds_bytes)
        eprint(f"[bonds] parsed records: {len(bonds_today)}")
    else:
        eprint("[bonds] ERROR: could not auto-detect bonds PDF URL. Set BONDS_PDF_URL env var.")

    # Rolling match by CID
    matches = []
    for b in bonds_today:
        cid = b.get("cid")
        if not cid:
            continue
        br = booked_by_cid.get(cid)
        if not br:
            continue
        matches.append({
            "cid": cid,
            "name": br.get("name") or b.get("name",""),
            "booking_no": br.get("booking_no",""),
            "book_in_date": br.get("book_in_date",""),
            "top_charge": br.get("top_charge","N/A"),
            "bond_set": b.get("bond_set",""),
            "mdate": b.get("mdate",""),
        })

    # Title date (fallback)
    title = "Tarrant County Jail Report"
    if not report_date:
        report_date = dt.datetime.now().strftime("%b %d, %Y")

    html_out = build_html(title, report_date, booked_today, bonds_today, matches, booked_days)

    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html_out)

    eprint(f"[ok] wrote {output_html}")

if __name__ == "__main__":
    main()
