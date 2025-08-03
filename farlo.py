import asyncio
import time
import re
import os
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# --- Google Sheets setup ---
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", SCOPE)
CLIENT = gspread.authorize(CREDS)

SPREADSHEET_KEY = os.environ.get("GSPREAD_SHEET_KEY")
if not SPREADSHEET_KEY:
    raise ValueError("Missing required environment variable: GSPREAD_SHEET_KEY")
SPREADSHEET = CLIENT.open_by_key(SPREADSHEET_KEY)

INPUT_SHEET = SPREADSHEET.worksheet("Sheet2")
OUTPUT_SHEET = SPREADSHEET.worksheet("Sheet1")

def timestamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_last_updated_time(row_idx):
    """Get the last updated timestamp for a product from the output sheet"""
    try:
        cell_value = OUTPUT_SHEET.cell(row_idx, 1).value
        if cell_value:
            return datetime.strptime(cell_value, "%Y-%m-%d %H:%M:%S")
    except:
        pass
    return None

def should_update(row_idx, interval_minutes):
    """Check if enough time has passed since last update"""
    last_update = get_last_updated_time(row_idx)
    if last_update is None:
        return True
    
    time_diff = datetime.now() - last_update
    return time_diff.total_seconds() >= (interval_minutes * 60)

async def scrape_with_retries(page, product, url, idx, retries=3, delay=5):
    for attempt in range(1, retries+1):
        try:
            # Navigate
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")

            # Bypass "Continue shopping" interstitial
            if await page.locator("button:has-text('Continue shopping')").is_visible():
                await page.click("button:has-text('Continue shopping')")
                await page.wait_for_timeout(2000)

            # Title
            title = (await page.locator("#productTitle").inner_text(timeout=5000)).strip()

            # Price
            price = (
                await page.locator("span.a-price > span.a-offscreen")
                              .first
                              .inner_text(timeout=5000)
            ).strip()
            price_clean = price.replace("₹", "").replace(",", "")

            # Seller & Quantity
            seller = "Unknown"
            qty = "Unknown"
            if await page.locator("#sellerProfileTriggerId").count():
                seller = (await page
                            .locator("#sellerProfileTriggerId")
                            .inner_text()).strip()
            else:
                mi = await page.locator("#merchant-info").inner_text()
                seller = mi.split("Sold by")[-1].split(".")[0].strip()

            opts = await page.locator("select#quantity option").all_inner_texts()
            nums = [int(o) for o in opts if o.strip().isdigit()]
            if nums:
                qty = "30+" if max(nums) >= 30 else str(max(nums))

            seller_qty = f"{seller} - {qty}"

            # Brand
            brand = "BRAND"
            if await page.locator("th:has-text('Brand') + td").count():
                brand = (await page
                          .locator("th:has-text('Brand') + td")
                          .inner_text()).strip().upper()
            else:
                m = re.match(r"([A-Z]+)", product)
                if m:
                    brand = m.group(1)

            # # of Offers on the listing
            offers = await page.locator(".olpOffer").count()

            # Write to sheet
            now = timestamp()
            row = [
                now,
                product,
                title,
                f"₹{price_clean}",
                url,
                seller_qty,
                brand,
                str(offers),
            ]
            OUTPUT_SHEET.update(f"A{idx}:H{idx}", [row])
            print(f"{now} – {product}: ₹{price_clean} | {seller_qty} | {brand} | offers: {offers}")
            return True

        except PlaywrightTimeoutError as e:
            print(f"[Retry {attempt}/{retries}] Timeout for {product}: {e}")
        except Exception as e:
            print(f"[Retry {attempt}/{retries}] Error for {product}: {e}")

        if attempt < retries:
            await asyncio.sleep(delay)

    # Permanent failure fallback
    now = timestamp()
    fallback = [
        now, product, "Error", "₹500", url, "Unknown - Unknown", "BRAND", "0"
    ]
    OUTPUT_SHEET.update(f"A{idx}:H{idx}", [fallback])
    print(f"✖ permanent fail: {product}")
    return False

async def main():
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        products  = INPUT_SHEET.col_values(1)[1:]
        urls      = INPUT_SHEET.col_values(2)[1:]
        intervals = INPUT_SHEET.col_values(3)[1:]

        scraped_count = 0
        for idx, (prod, url, ival) in enumerate(zip(products, urls, intervals), start=2):
            interval = int(ival.strip()) if ival.strip().isdigit() else 60
            
            if should_update(idx, interval):
                print(f"Scraping {prod} (interval: {interval}min)")
                ok = await scrape_with_retries(page, prod, url, idx)
                if ok:
                    scraped_count += 1
            else:
                print(f"Skipping {prod} (not due for update)")

        await browser.close()
        print(f"Scraping complete. Updated {scraped_count} products.")

if __name__ == "__main__":
    asyncio.run(main())
