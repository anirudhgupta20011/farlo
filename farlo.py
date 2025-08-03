import asyncio, re, random, os
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.async_api import async_playwright

# --- CONFIG -----------------------------------------------------------------------------
CREDENTIALS_FILE = "credentials.json"
GSHEET_ID = os.environ.get("GSPREAD_SHEET_KEY")
if not GSHEET_ID:
    raise ValueError("Missing required environment variable: GSPREAD_SHEET_KEY")
INPUT_SHEET      = "Sheet2"
OUTPUT_SHEET     = "Sheet1"

RETRIES      = 3     # number of retries per SKU
RETRY_DELAY  = 5     # seconds between retries

# A small pool of real Chrome UAs to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]

# --- GOOGLE SHEETS SETUP ----------------------------------------------------------------
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds  = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
gc     = gspread.authorize(creds)
sheet  = gc.open_by_key(GSHEET_ID)
in_ws  = sheet.worksheet(INPUT_SHEET)
out_ws = sheet.worksheet(OUTPUT_SHEET)

# --- SCRAPE + WRITE WITH RETRIES ---------------------------------------------------------
async def scrape_and_write(page, idx, product, url):
    for attempt in range(1, RETRIES + 1):
        try:
            # Navigate & wait
            await page.goto(url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")
            # tiny randomized stealth delay
            await page.wait_for_timeout(random.uniform(500, 1000))

            # --- Title ---
            title_loc = page.locator("#productTitle").first
            title = (await title_loc.inner_text()).strip() if await title_loc.count() else "N/A"

            # --- Price ---
            price_loc = page.locator("span.a-price > span.a-offscreen").first
            price_txt = (await price_loc.inner_text()).strip() if await price_loc.count() else "500"
            price_val = price_txt.replace("₹", "").replace(",", "").strip()

            # --- Seller ---
            seller = "Unknown"
            if await page.locator("#sellerProfileTriggerId").count():
                seller = (await page.locator("#sellerProfileTriggerId").first.inner_text()).strip()
            else:
                mi = page.locator("#merchant-info").first
                if await mi.count():
                    txt = (await mi.inner_text()).strip()
                    m = re.search(r"Sold by\s+([^\.]+)", txt)
                    if m:
                        seller = m.group(1).strip()

            # --- Quantity in Buy Box ---
            opts = await page.locator("select#quantity option").all_inner_texts()
            nums = [int(m.group(1)) for o in opts if (m := re.search(r"(\d+)", o))]
            qty = "30+" if nums and max(nums) >= 30 else str(max(nums)) if nums else "Unknown"
            seller_qty = f"{seller} – {qty}"

            # --- Brand ---
            brand = "Brand"
            b_loc = page.locator("th:has-text('Brand') + td").first
            if await b_loc.count():
                brand = (await b_loc.inner_text()).strip().upper()
            else:
                m = re.match(r"([A-Z]+)", product)
                if m:
                    brand = m.group(1)

            # --- Total # of Offers ---
            offers = "1"
            o_loc = page.locator("a[href*='/gp/offer-listing/']").first
            if await o_loc.count():
                txt = await o_loc.inner_text()
                mo = re.search(r"(\d+)", txt.replace(",", ""))
                if mo:
                    offers = mo.group(1)

            # --- Write back A–H ---
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            row = [
                now,
                product,
                title,
                f"₹{price_val}",
                url,
                seller_qty,
                brand,
                offers
            ]
            out_ws.update(f"A{idx}:H{idx}", [row])
            print(f"{now} – {product}: ₹{price_val} | {seller_qty} | {brand} | offers: {offers}")
            return True

        except Exception as e:
            print(f"[Retry {attempt}/{RETRIES}] {product}: {e}")
            if attempt < RETRIES:
                await asyncio.sleep(RETRY_DELAY)
            else:
                print(f"Failed after retries: {product}")
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                fallback = [
                    now, product, "Error", "₹500", url,
                    "Unknown – Unknown", "Brand", "0"
                ]
                out_ws.update(f"A{idx}:H{idx}", [fallback])
                return False

# --- MAIN LOOP -----------------------------------------------------------------------------
async def main():
    async with async_playwright() as pw:
        # launch once with rotating UA + stealth
        browser = await pw.chromium.launch(headless=True)
        ua = random.choice(USER_AGENTS)
        context = await browser.new_context(user_agent=ua, locale="en-US")
        await context.add_init_script("""
            () => { Object.defineProperty(navigator, 'webdriver', {get: () => undefined}); }
        """)
        page = await context.new_page()

        data    = in_ws.get_all_values()

        for i, line in enumerate(data[1:], start=2):
            if len(line) < 3 or not line[0].strip() or not line[1].strip():
                continue

            product, url, _ = line[:3]
            await scrape_and_write(page, i, product, url)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
