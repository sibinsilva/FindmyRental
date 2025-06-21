import sqlite3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import csv
import os
import schedule
import time
import re

# Load environment variables from .env file
load_dotenv()

# Get environment variables
BASE_PATH = os.getenv("BASE_PATH")
BASE_TYPE = os.getenv("BASE_TYPE")
RADIUS = int(os.getenv("RADIUS"))
CSV_FILE = os.getenv("CSV_FILE")
DB_FILE = os.getenv("DB_FILE")



def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("start-maximized")
    options.add_argument("--enable-unsafe-swiftshader")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/137.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    return driver


def extract(page, driver=None):
    url = f'https://www.daft.ie/{BASE_TYPE}/{BASE_PATH}?radius={RADIUS}&page={page}'

    close_driver = False
    if driver is None:
        driver = setup_driver()
        close_driver = True

    driver.get(url)
    wait = WebDriverWait(driver, 15)

    if page == 1:
        try:
            accept_button = wait.until(
                EC.element_to_be_clickable((By.ID, "didomi-notice-agree-button"))
            )
            accept_button.click()
            print("Clicked 'Accept All' cookie button.")
        except Exception:
            print("No cookie consent popup or already accepted.")

    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "ul[data-testid='results']"))
    )

    soup = BeautifulSoup(driver.page_source, 'html.parser')

    max_page = get_max_page(soup)

    if close_driver:
        driver.quit()

    return soup, max_page


def parse_listings(soup):
    listings = soup.select("li[data-testid^='result-']")
    data_rows = []

    for listing in listings:
        data = {}

        a_tag = listing.find("a", href=True)
        if a_tag:
            data["url"] = "https://www.daft.ie" + a_tag["href"]

        title = listing.find("p", attrs={"data-tracking": "srp_address"})
        data["title"] = title.text.strip() if title else None

         # If title is None, extract slug from URL path
        if not title and data["url"]:
            try:
                # Extract slug from URL path
                # Example URL: https://www.daft.ie/for-rent/apartment-city-centre-dublin/1234567
                # Slug: apartment-city-centre-dublin
                path = data["url"].split('/for-rent/')[-1]
                slug = path.rsplit('/', 1)[0]      
                title = slug.replace('-', ' ').strip()  
            except Exception:
                title = None

        data["title"] = title

        price_tag = listing.find("p", string=lambda x: x and "€" in x)
        data["price"] = price_tag.text.strip() if price_tag else None

        specs = listing.find_all("span")
        bed_bath = [s.text for s in specs if "Bed" in s.text or "Bath" in s.text]
        if not bed_bath:
            bed_bath = ["Studio"]
        data["bed_bath"] = bed_bath

        data_rows.append(data)
    return data_rows


def get_max_page(soup):
    pagination_ul = soup.find("ul", class_="sc-b634e258-1 facluD")
    if not pagination_ul:
        print("Pagination ul not found.")
        return None

    page_numbers = []
    for li in pagination_ul.find_all("li"):
        text = li.text.strip()
        if text.isdigit():
            page_numbers.append(int(text))

    if page_numbers:
        max_page = max(page_numbers)
        return max_page
    else:
        print("No page numbers found inside pagination ul.")
        return None

def create_table():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE,
            title TEXT,
            price TEXT,
            bed_bath TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_to_db(data_rows):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    new_count = 0
    for row in data_rows:
        try:
            c.execute('''
                INSERT INTO listings (url, title, price, bed_bath)
                VALUES (?, ?, ?, ?)
            ''', (
                row.get("url"),
                row.get("title"),
                row.get("price"),
                ', '.join(row.get("bed_bath", []))
            ))
            new_count += 1
        except sqlite3.IntegrityError:
            # Duplicate URL (already exists)
            continue

    conn.commit()
    conn.close()
    print(f"✅ Added {new_count} new listings to database.")


def save_new_listings(data_rows, csv_file):
    existing_urls = set()
    if os.path.exists(csv_file):
        with open(csv_file, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_urls.add(row.get("url"))

    new_data = []
    for row in data_rows:
        url = row.get("url")
        if url and url not in existing_urls:
            existing_urls.add(url)
            new_data.append(row)

    file_exists = os.path.exists(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["title", "price", "bed_bath", "url"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_data)

    print(f"✅ Added {len(new_data)} new listings to {csv_file}")

def parse_price(price_str):
    if not price_str:
        return float('inf')  # treat missing prices as very high for sorting at end
    # Extract digits and commas, remove € and text
    # Example price_str: "€1,956 per month"
    match = re.search(r'[\d,]+', price_str)
    if match:
        # Remove commas and convert to int
        return int(match.group(0).replace(',', ''))
    return float('inf')


def run_scraper():
    print("Starting scraper...")
    create_table()
    all_data = []
    driver = setup_driver()

    soup, max_page = extract(1, driver=driver)
    if max_page is None:
        max_page = 1

    print(f"Found max page: {max_page}")

    listings_data = parse_listings(soup)
    print(f"Found {len(listings_data)} listings on page 1")
    all_data.extend(listings_data)

    for page_num in range(2, max_page + 1):
        print(f"Extracting page {page_num} of {max_page}...")
        soup, _ = extract(page_num, driver=driver)
        listings_data = parse_listings(soup)
        print(f"Found {len(listings_data)} listings on page {page_num}")
        all_data.extend(listings_data)

    driver.quit()

    for listing in all_data:
        print(listing)

    # Sort all_data by numeric price ascending
    all_data.sort(key=lambda x: parse_price(x.get("price")))

    #save_new_listings(all_data, CSV_FILE)
    save_to_db(all_data)
    print("Scraper finished.")


if __name__ == "__main__":
    run_scraper()  # Run once immediately on start

    # Schedule to run every 24 hours
    schedule.every(24).hours.do(run_scraper)

    while True:
        schedule.run_pending()
        time.sleep(60)  # wait 1 minute before checking again
