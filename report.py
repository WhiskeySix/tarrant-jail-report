import os
import requests
import asyncio
from datetime import datetime, timedelta
from pyppeteer import launch

# =========================
# ENV VARIABLES
# =========================
BASE44_AUTOMATION_API_KEY = os.getenv("BASE44_AUTOMATION_API_KEY", "").strip()
BASE44_FUNCTION_URL = os.getenv("BASE44_FUNCTION_URL", "").strip()

PDF_OUTPUT_PATH = "output/daily_jail_report.pdf"

# =========================
# BASE44 SYNC FUNCTION
# =========================
def send_report_to_base44(report_payload: dict):
    if not BASE44_FUNCTION_URL:
        print("WARNING: Missing BASE44_FUNCTION_URL. Skipping Base44 sync.")
        return

    if not BASE44_AUTOMATION_API_KEY:
        print("WARNING: Missing BASE44_AUTOMATION_API_KEY. Skipping Base44 sync.")
        return

    print("Sending latest report data to Base44...")

    try:
        response = requests.post(
            BASE44_FUNCTION_URL,
            headers={
                "Content-Type": "application/json",
                "x-automation-api-key": BASE44_AUTOMATION_API_KEY,
            },
            json=report_payload,
            timeout=60,
        )

        print("Base44 sync status:", response.status_code)
        print("Base44 response:", response.text[:1000])

        response.raise_for_status()
        print("Base44 report sync completed successfully.")

    except Exception as e:
        print(f"ERROR: Base44 report sync failed: {e}")

# =========================
# PDF GENERATOR (FIXED)
# =========================
async def generate_pdf_from_html(html_content: str):
    print("Generating PDF from HTML...")
    browser = None

    try:
        browser = await launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
            ],
            handleSIGINT=False,
            handleSIGTERM=False,
            handleSIGHUP=False,
        )

        page = await browser.newPage()

        # ✅ FIXED (no extra args)
        await page.setContent(html_content)

        await page.pdf({
            "path": PDF_OUTPUT_PATH,
            "format": "Letter",
            "printBackground": True,
            "margin": {
                "top": "0.5in",
                "right": "0.5in",
                "bottom": "0.5in",
                "left": "0.5in",
            },
        })

        print("PDF exists?", os.path.exists(PDF_OUTPUT_PATH))

    except Exception as e:
        print(f"ERROR: PDF generation failed: {e}")

    finally:
        if browser:
            try:
                await browser.close()
            except Exception as close_error:
                print(f"WARNING: Browser close failed: {close_error}")

# =========================
# MAIN (SIMPLIFIED EXAMPLE)
# =========================
def main():
    print("---- Starting Tarrant County Daily Jail Report ----")

    # ⛔️ IMPORTANT:
    # Replace this block with your ACTUAL parsed data
    # This is just a safe placeholder structure

    today = datetime.now()
    yesterday = today - timedelta(days=1)

    report_date_str = today.strftime("%-m/%-d/%Y")
    arrests_date_str = yesterday.strftime("%-m/%-d/%Y")

    # Example data (replace with your real parsed data)
    report_payload = {
        "report_date": report_date_str,
        "arrests_date": arrests_date_str,
        "total_bookings": 123,
        "top_charge": "POSS CS PG 1/1-B <1G",
        "charge_mix": [
            {"label": "Drugs / Possession", "pct": 28, "count": 35},
        ],
        "cities": [
            {"city": "Fort Worth", "pct": 45, "count": 55},
        ],
        "bookings": [
            {
                "num": 1,
                "name": "DOE, JOHN",
                "date": arrests_date_str,
                "charges": "EXAMPLE CHARGE",
                "city": "Fort Worth"
            }
        ],
        "is_active": True
    }

    # =========================
    # SEND TO BASE44
    # =========================
    send_report_to_base44(report_payload)

    print("---- Done ----")

# =========================
# RUN
# =========================
if __name__ == "__main__":
    main()
