import os
import re
import ssl
import smtplib
import datetime as dt
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from urllib.request import Request, urlopen

import pdfplumber


# -----------------------------
# Helpers
# -----------------------------
def env(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def http_download(url: str, timeout: int = 60) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def extract_pdf_text(pdf_bytes: bytes) -> list[str]:
    pages = []
    with pdfplumber.open(io_bytes(pdf_bytes)) as pdf:
        for p in pdf.pages:
            t = p.extract_text() or ""
            pages.append(t)
    return pages


class io_bytes:
    # tiny wrapper so pdfplumber can open bytes without needing tempfile
    def __init__(self, b: bytes):
        self._b = b
        self._i = 0

    def read(self, n: int = -1) -> bytes:
        if n == -1:
            n = len(self._b) - self._i
        chunk = self._b[self._i : self._i + n]
        self._i += len(chunk)
        return chunk

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 0:
            self._i = offset
        elif whence == 1:
            self._i += offset
        else:
            self._i = len(self._b) + offset
        return self._i

    def tell(self) -> int:
        return self._i


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_mmddyyyy(s: str) -> dt.date | None:
    try:
        m, d, y = s.split("/")
        return dt.date(int(y), int(m), int(d))
    except Exception:
        return None


# -----------------------------
# Parsing: BOOKED-IN
# Source: https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF/01.PDF
#
# Text structure per record (commonly):
#   NAME ... CID  MM/DD/YYYY
#   Address line  BOOKINGNO  DESCRIPTION
#   City/state/zip
#   (sometimes additional descriptions)
# -----------------------------
BOOKING_NO_RE = re.compile(r"\b\d{2}-\d{7}\b")
DATE_RE = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
CID_RE = re.compile(r"\b\d{6,8}\b")
NAME_RE = re.compile(r"^[A-Z][A-Z' \-]+,\s*[A-Z][A-Z' \-]+")


def is_booked_header_line(line: str) -> bool:
    line = norm_space(line)
    if not line:
        return False
    if "Inmates Booked In During the Past 24 Hours" in line:
        return False
    if line.startswith("Inmate Name Identifier CID"):
        return False
    # Must begin with LAST, FIRST (uppercase) and contain CID and a date
    if not NAME_RE.match(line):
        return False
    if not CID_RE.search(line):
        return False
    if not DATE_RE.search(line):
        return False
    return True


def parse_booked_records(pages_text: list[str]) -> list[dict]:
    lines = []
    for page in pages_text:
        for raw in (page or "").splitlines():
            l = raw.rstrip()
            if l.strip():
                lines.append(l)

    records: list[dict] = []
    current: dict | None = None

    for raw in lines:
        line = norm_space(raw)

        if is_booked_header_line(line):
            # flush prior
            if current:
                records.append(current)
            # start new
            cid = CID_RE.search(line).group()
            date_str = DATE_RE.search(line).group()
            name = line
            # remove cid/date from name line for cleaner display
            name = norm_space(
                re.sub(r"\b" + re.escape(cid) + r"\b", "", name)
            )
            name = norm_space(
                re.sub(r"\b" + re.escape(date_str) + r"\b", "", name)
            )
            current = {
                "name": name,
                "cid": cid,
                "book_in_date": date_str,
                "booking_no": "",
                "top_charge": "",
            }
            continue

        if not current:
            continue

        # Find booking number + charge on subsequent lines
        m = BOOKING_NO_RE.search(line)
        if m and not current["booking_no"]:
            current["booking_no"] = m.group()
            after = norm_space(line.split(m.group(), 1)[1] if m.group() in line else "")
            # Anything after booking # on that line is usually the first (top) charge
            if after and not current["top_charge"]:
                current["top_charge"] = after
            continue

        # If we already have a booking # but no top charge, capture first meaningful next line
        if current["booking_no"] and not current["top_charge"]:
            # Skip obvious address/city lines
            if " TX " in line or re.search(r"\bTX\s+\d{5}\b", line):
                continue
            if len(line) > 8 and line.upper() == line:
                current["top_charge"] = line
                continue

    if current:
        records.append(current)

    # Final cleanup: drop anything that somehow lacks CID
    out = []
    for r in records:
        if r.get("cid"):
            out.append(r)
    return out


# -----------------------------
# Parsing: BONDS ISSUED (bond amounts SET)
# Source: https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF
#
# Typical extracted line often contains:
#   BONDNO STATUS AMOUNT ... CID NAME OFFENSE ... MDATE ...
# -----------------------------
AMOUNT_RE = re.compile(r"\b\d{1,3}(?:,\d{3})*\.\d{2}\b")


def parse_bonds_records(pages_text: list[str]) -> list[dict]:
    lines = []
    for page in pages_text:
        for raw in (page or "").splitlines():
            l = raw.rstrip()
            if l.strip():
                lines.append(norm_space(l))

    records: list[dict] = []
    i = 0
    while i < len(lines):
        line = lines[i]

        # skip headers
        if line.startswith("List of Bonds Issued Over"):
            i += 1
            continue
        if line.startswith("Bond Number Status"):
            i += 1
            continue

        cid_m = CID_RE.search(line)
        amt_m = AMOUNT_RE.search(line)
        name_m = re.search(r"\b([A-Z][A-Z' \-]+,\s*[A-Z][A-Z' \-]+)\b", line)

        # A "good" row usually has these 3 on the same line
        if cid_m and amt_m and name_m:
            cid = cid_m.group()
            amount = amt_m.group()
            name = norm_space(name_m.group(1))

            # offense: everything after the name, plus maybe continuation lines until next record
            after_name = norm_space(line.split(name_m.group(1), 1)[1])
            # remove any trailing obvious columns we don’t want as offense
            # (we’ll separately capture mdate if present)
            mdate = ""
            d = DATE_RE.search(after_name)
            if d:
                mdate = d.group()
                # offense is text before first date (usually)
                offense = norm_space(after_name.split(mdate, 1)[0])
            else:
                offense = after_name

            # If offense still empty or looks like just "TX 761xx", try pulling next line(s)
            if not offense or offense in ("TX", "N/A"):
                j = i + 1
                extra = []
                while j < len(lines):
                    nxt = lines[j]
                    # stop if next looks like another record
                    if CID_RE.search(nxt) and AMOUNT_RE.search(nxt) and re.search(r",", nxt):
                        break
                    # keep only ALLCAPS-ish offense lines
                    if len(nxt) > 8 and nxt.upper() == nxt and "Page:" not in nxt:
                        extra.append(nxt)
                    j += 1
                if extra:
                    offense = norm_space(" ".join(extra))

            records.append(
                {
                    "cid": cid,
                    "name": name,
                    "amount": amount,
                    "offense": offense if offense else "N/A",
                    "mdate": mdate if mdate else "N/A",
                }
            )

        i += 1

    # Deduplicate (same CID+amount+offense can appear across wrapped pages)
    seen = set()
    out = []
    for r in records:
        key = (r["cid"], r["amount"], r["offense"], r["mdate"], r["name"])
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


# -----------------------------
# Reporting logic
# -----------------------------
def top_charges_from_booked(booked: list[dict], top_n: int = 3) -> list[str]:
    counts = {}
    for r in booked:
        c = r.get("top_charge") or ""
        c = norm_space(c)
        if not c:
            continue
        counts[c] = counts.get(c, 0) + 1
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return [x[0] for x in ranked[:top_n]]


def build_table(headers: list[str], rows: list[list[str]]) -> str:
    th = "".join(f"<th>{h}</th>" for h in headers)
    trs = []
    for row in rows:
        tds = "".join(f"<td>{(c or 'N/A')}</td>" for c in row)
        trs.append(f"<tr>{tds}</tr>")
    return f"""
    <table>
      <thead><tr>{th}</tr></thead>
      <tbody>
        {''.join(trs) if trs else '<tr><td colspan="' + str(len(headers)) + '">No records</td></tr>'}
      </tbody>
    </table>
    """


def build_email_html(report_date: str, booked_today: list[dict], bonds_today: list[dict], matches: list[dict]) -> str:
    css = """
    <style>
      body { font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; background:#0b0b0c; color:#f5f5f7; margin:0; padding:24px; }
      h1 { font-size: 28px; margin: 0 0 12px 0; }
      h2 { font-size: 20px; margin: 22px 0 10px 0; }
      .sub { color:#c7c7cc; margin: 0 0 18px 0; }
      .kpi { margin: 10px 0; font-size: 16px; }
      .pill { display:inline-block; padding:6px 10px; border:1px solid #2c2c2e; border-radius:999px; margin:4px 6px 0 0; color:#f5f5f7; }
      table { width:100%; border-collapse: collapse; margin-top: 10px; }
      th, td { border:1px solid #2c2c2e; padding:10px; vertical-align: top; }
      th { background:#1c1c1e; text-align:left; }
      td { background:#0f0f10; }
      .note { color:#c7c7cc; font-size: 13px; line-height: 1.4; margin-top: 10px; }
      .hr { height:1px; background:#2c2c2e; margin: 18px 0; }
      .muted { color:#c7c7cc; }
    </style>
    """

    # Section 1: New Bookings (Last 24 hours = Day 01 Booked-In PDF)
    top_charges = top_charges_from_booked(booked_today, top_n=3)
    top_charges_html = "".join(f"<span class='pill'>{c}</span>" for c in top_charges) if top_charges else "<span class='muted'>N/A</span>"

    booked_rows = []
    for r in booked_today[:25]:
        booked_rows.append([r.get("name","N/A"), r.get("booking_no","N/A") or "N/A", r.get("top_charge","N/A") or "N/A"])

    # Section 2: New Bonds Set (Last 24 hours = Day 01 Bonds PDF)
    bonds_rows = []
    for r in bonds_today[:25]:
        bonds_rows.append([r.get("name","N/A"), r.get("offense","N/A"), r.get("amount","N/A"), r.get("mdate","N/A")])

    # Section 3: Rolling Match (Bonds day 01 vs Booked-In day 01..03 by CID)
    match_rows = []
    for m in matches[:50]:
        match_rows.append([
            m.get("name","N/A"),
            m.get("booking_no","N/A"),
            m.get("book_in_date","N/A"),
            m.get("top_charge","N/A"),
            m.get("bond_amount","N/A"),
            m.get("mdate","N/A"),
        ])

    html = f"""
    <html>
    <head>{css}</head>
    <body>
      <h1>Tarrant County Jail Report — {report_date}</h1>
      <p class="sub">Automated summary from Tarrant County CJ Reports PDFs.</p>

      <div class="hr"></div>

      <h2>1) New Bookings (Last 24 Hours)</h2>
      <div class="kpi"><b>{len(booked_today)}</b> new bookings parsed from Booked-In Day 01 PDF</div>
      <div class="kpi"><b>Top charges:</b> {top_charges_html}</div>
      {build_table(["Name","Booking No","Top Charge"], booked_rows)}

      <div class="hr"></div>

      <h2>2) New Bonds Set (Last 24 Hours)</h2>
      <div class="kpi"><b>{len(bonds_today)}</b> bonds set parsed from Bonds Day 01 PDF</div>
      {build_table(["Name","Offense (Bonds Report)","Bond Set","MDate"], bonds_rows)}
      <p class="note">
        Note: The Bonds Issued report reflects bond amounts <b>set</b> in the last 24 hours. It does not indicate bond payment, release, or custody status.
      </p>

      <div class="hr"></div>

      <h2>3) Rolling Match (Bonds Set vs. Booked-In Last 3 Days)</h2>
      <div class="kpi"><b>{len(matches)}</b> bond records matched to someone booked-in within the last 3 days (matched by CID)</div>
      {build_table(["Name","Booking No","Book In Date","Top Charge","Bond Set","MDate"], match_rows)}
      <p class="note">
        Matching is done by CID across a rolling 3-day window because bond setting often occurs after booking (day-lag is common).
      </p>

    </body>
    </html>
    """
    return html


def send_email(subject: str, html_body: str, to_email: str, smtp_user: str, smtp_pass: str):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html"))

    context = ssl.create_default_context()
    with smtplib.SMTP("smtp.gmail.com", 587, timeout=60) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, [to_email], msg.as_string())


# -----------------------------
# Main
# -----------------------------
def url_for_day(base: str, day: int) -> str:
    base = base.rstrip("/")
    return f"{base}/{day:02d}.PDF"


def derive_base_from_pdf_url(pdf_url: str) -> str:
    # If user supplies full .../01.PDF, turn it into base .../FinalPDF
    if pdf_url.upper().endswith(".PDF"):
        return re.sub(r"/\d{2}\.PDF$", "", pdf_url, flags=re.IGNORECASE).rstrip("/")
    return pdf_url.rstrip("/")


def main():
    # Secrets / env
    to_email = env("TO_EMAIL")
    smtp_user = env("SMTP_USER")
    smtp_pass = env("SMTP_PASS")

    booked_base = env("BOOKED_BASE_URL", "https://cjreports.tarrantcounty.com/Reports/JailedInmates/FinalPDF")
    booked_days = int(env("BOOKED_DAYS", "3"))

    bonds_pdf_url = env("BONDS_PDF_URL", "https://cjreports.tarrantcounty.com/Reports/BondsIssued/FinalPDF/01.PDF")
    bonds_base = derive_base_from_pdf_url(bonds_pdf_url)

    if not (to_email and smtp_user and smtp_pass):
        raise RuntimeError("Missing TO_EMAIL / SMTP_USER / SMTP_PASS env vars.")

    # -----------------------------
    # Download + parse BOOKED-IN (day 01..N)
    # -----------------------------
    booked_all: list[dict] = []
    booked_day01: list[dict] = []

    print(f"BOOKED_BASE_URL: {booked_base}")
    print(f"BOOKED_DAYS: {booked_days}")

    for d in range(1, booked_days + 1):
        u = url_for_day(booked_base, d)
        print(f"[booked-in] downloading: {u}")
        pdf_bytes = http_download(u)
        pages = []
        with pdfplumber.open(io_bytes(pdf_bytes)) as pdf:
            for p in pdf.pages:
                pages.append(p.extract_text() or "")
        recs = parse_booked_records(pages)
        print(f"[booked-in] day {d:02d} parsed records: {len(recs)}")
        booked_all.extend(recs)
        if d == 1:
            booked_day01 = recs

    # Make CID -> "best" booked record (prefer one that has booking # and charge)
    booked_by_cid: dict[str, dict] = {}
    for r in booked_all:
        cid = r.get("cid", "")
        if not cid:
            continue
        # choose the record with the most filled fields
        score = int(bool(r.get("booking_no"))) + int(bool(r.get("top_charge")))
        prev = booked_by_cid.get(cid)
        if not prev:
            booked_by_cid[cid] = r
        else:
            prev_score = int(bool(prev.get("booking_no"))) + int(bool(prev.get("top_charge")))
            if score > prev_score:
                booked_by_cid[cid] = r

    # Determine report date from Booked-in Day 01 header (if present)
    report_date = dt.date.today().strftime("%b %d, %Y")
    # try to find "Report Date: M/D/YYYY" in first page text
    if booked_day01:
        pass  # keep derived below
    # parse from any page header line if present
    for page in (pages if "pages" in locals() else []):
        m = re.search(r"Report Date:\s*(\d{1,2}/\d{1,2}/\d{4})", page)
        if m:
            d = parse_mmddyyyy(m.group(1))
            if d:
                report_date = d.strftime("%b %d, %Y")
            break

    # -----------------------------
    # Download + parse BONDS (day 01 only = last 24 hours)
    # -----------------------------
    bonds_url = url_for_day(bonds_base, 1)
    print(f"[bonds] downloading: {bonds_url}")
    bonds_pdf = http_download(bonds_url)
    bonds_pages = []
    with pdfplumber.open(io_bytes(bonds_pdf)) as pdf:
        for p in pdf.pages:
            bonds_pages.append(p.extract_text() or "")
    bonds_day01 = parse_bonds_records(bonds_pages)
    print(f"[bonds] day 01 parsed records: {len(bonds_day01)}")

    # -----------------------------
    # Rolling match (bonds day 01 vs booked last N days by CID)
    # -----------------------------
    matches = []
    for b in bonds_day01:
        cid = b.get("cid", "")
        if not cid:
            continue
        if cid in booked_by_cid:
            br = booked_by_cid[cid]
            matches.append(
                {
                    "cid": cid,
                    "name": br.get("name") or b.get("name") or "N/A",
                    "booking_no": br.get("booking_no") or "N/A",
                    "book_in_date": br.get("book_in_date") or "N/A",
                    "top_charge": br.get("top_charge") or "N/A",
                    "bond_amount": b.get("amount") or "N/A",
                    "mdate": b.get("mdate") or "N/A",
                }
            )

    # Sort matches: biggest bond first (best effort numeric)
    def amt_key(x: dict) -> float:
        s = (x.get("bond_amount") or "").replace(",", "")
        try:
            return float(s)
        except Exception:
            return 0.0

    matches.sort(key=lambda x: amt_key(x), reverse=True)

    # -----------------------------
    # Build + Send email
    # -----------------------------
    subject = f"Tarrant County Jail Report — {report_date}"
    html = build_email_html(report_date, booked_day01, bonds_day01, matches)

    print("[email] sending...")
    send_email(subject, html, to_email, smtp_user, smtp_pass)
    print("[email] sent OK")


if __name__ == "__main__":
    main()
