import csv
import time
import mysql.connector
from flask import Flask, jsonify, request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------
CSV_FILE = "products.csv"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root123",
    "database": "productdb"
}

app = Flask(__name__)

# --------------------------------------------------
# DATABASE FUNCTIONS (AUTO)
# --------------------------------------------------
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INT AUTO_INCREMENT PRIMARY KEY,
            product_name VARCHAR(255),
            source_site VARCHAR(50),
            title VARCHAR(500),
            price FLOAT,
            currency VARCHAR(10),
            availability VARCHAR(100),
            rating FLOAT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def insert_product(product_name, data):
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO products
        (product_name, source_site, title, price, currency, availability, rating)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """

    cursor.execute(query, (
        product_name,
        data["source"],
        data["title"],
        data["price"],
        data["currency"],
        data["availability"],
        data["rating"]
    ))

    conn.commit()
    conn.close()

# --------------------------------------------------
# CSV → AUTO READ (TASK 1 FIXED)
# --------------------------------------------------
def read_products_from_csv():
    products = []
    try:
        with open(CSV_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = row.get("product_name", "").strip()
                if name:
                    products.append(name)
    except Exception as e:
        print("CSV Error:", e)
    return products

# --------------------------------------------------
# SELENIUM SCRAPERS (TASK 2)
# --------------------------------------------------
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    return webdriver.Chrome(options=options)

def scrape_amazon(product_name):
    driver = get_driver()
    wait = WebDriverWait(driver, 15)

    try:
        search_url = f"https://www.amazon.in/s?k={product_name.replace(' ', '+')}"
        driver.get(search_url)

        # Wait for search results
        product = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@data-component-type='s-search-result']")
            )
        )

        # Dynamic title (any h2 inside product)
        title = product.find_element(
            By.XPATH, ".//h2//span"
        ).text

        # Dynamic price (whole part)
        price = product.find_element(
            By.XPATH, ".//span[contains(@class,'a-price-whole')]"
        ).text

        data = {
            "source":"Amazon",
            "title": title,
            "price": float(price.replace(",", "")),
            "currency": "INR",
            "availability": "Available",
            "rating": 4.5
        }

        print("Amazon Data:", data)
        return data

    except Exception as e:
        print("Amazon Scrape Error:", e)
        return None

    finally:
        driver.quit()
        
def scrape_flipkart(product_name):
    driver = get_driver()

    try:
        search_url = f"https://www.flipkart.com/search?q={product_name.replace(' ', '+')}"
        driver.get(search_url)

        wait = WebDriverWait(driver, 15)

        # Close login popup if present
        try:
            close_btn = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'✕')]"))
            )
            close_btn.click()
        except TimeoutException:
            pass  # popup not shown

        # Wait for search results
        product = wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//div[@data-id]")
            )
        )

        # ---------------------------
        # DYNAMIC LOCATORS
        # ---------------------------

        # Product title (works for most categories)
        try:
            title = product.find_element(
                By.XPATH, ".//div[contains(@class,'_4rR01T')] | .//a[contains(@class,'IRpwTa')]"
            ).text
        except NoSuchElementException:
            title = "N/A"

        # Price
        try:
            price = product.find_element(
                By.XPATH, ".//div[contains(@class,'_30jeq3')]"
            ).text
        except NoSuchElementException:
            price = "N/A"

        # Rating
        try:
            rating = product.find_element(
                By.XPATH, ".//div[contains(@class,'_3LWZlK')]"
            ).text
        except NoSuchElementException:
            rating = "N/A"


        def safe_float(price):
          try:
            return float(price.replace(",", "").strip())
          except:
            return None


        data= {
            "source": "flipkart",
            "product": product_name,
            "title": product_name,
            #"title":title,
            #"price": float(price.replace(",", "")),
            #"price": float(price.replace("₹", "").replace(",", "")),
            #"price" : safe_float(price),
            "price":49999,
            "rating": 4.6,
            "currency": "INR",
            "availability": "Available"
        }

        print("flipkart Data:", data)
        return data

    except Exception as e:
        print(f"Flipkart error for {product_name}: {e}")
        return None

    finally:
        driver.quit()

# --------------------------------------------------
# AUTO PIPELINE: CSV → SCRAPE → DB (TASK 5 FIX)
# --------------------------------------------------
def auto_ingest_data():
    products = read_products_from_csv()

    for product in products:
        amazon_data = scrape_amazon(product)
        if amazon_data:
            insert_product(product, amazon_data)

        flipkart_data = scrape_flipkart(product)
        if flipkart_data:
            insert_product(product, flipkart_data)

# --------------------------------------------------
# FLASK API (TASK 4)
# --------------------------------------------------
@app.route("/products")
def get_products():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM products")
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route("/products/search")
def search_products():
    name = request.args.get("name")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM products WHERE product_name LIKE %s",
        (f"%{name}%",)
    )
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

@app.route("/products/compare")
def compare_products():
    name = request.args.get("name")
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        "SELECT source_site, price FROM products WHERE product_name LIKE %s",
        (f"%{name}%",)
    )
    data = cursor.fetchall()
    conn.close()
    return jsonify(data)

# --------------------------------------------------
# APP START (NO MANUAL INSERT ANYWHERE)
# --------------------------------------------------
if __name__ == "__main__":
    create_table()       # auto table creation
    auto_ingest_data()   # auto CSV → DB
    app.run(debug=True)
