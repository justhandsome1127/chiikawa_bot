#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime

# 設定日誌記錄檔及等級
logging.basicConfig(
    filename='scraper.log', 
    level=logging.INFO, 
    format='%(asctime)s %(levelname)s: %(message)s'
)

# 建立資料庫連線函式
def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="chiikawa_user",        
            password="Chiikawa@123",    
            database="chiikawa_db"
        )
        return connection
    except Error as e:
        logging.error("資料庫連線失敗: %s", e)
        return None

# 從指定分頁抓取商品資料
def fetch_products(page):
    url = f'https://chiikawamarket.jp/collections/all?page={page}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error("網頁請求失敗 (Page %s): %s", page, e)
        return []
    
    soup = BeautifulSoup(response.text, 'html.parser')
    # 根據實際網頁結構調整 CSS 選擇器（此處假設商品區塊有 .product-grid-item）
    products = soup.select('div.product--root')
    return products

# 解析單一商品，取得名稱、圖片 URL 與庫存狀態
def parse_product(product):
    try:
        # 請根據網頁實際 HTML 結構調整 CSS 選擇器
        name_tag = product.select_one("h2.product_name")
        name = name_tag.get_text(strip=True) if name_tag else "未知商品"
        
        # 取得圖片 URL，優先從 <noscript> 區塊中的 img 取得
        noscript_img = product.select_one("noscript img")
        if noscript_img and noscript_img.has_attr("src"):
            image_url = noscript_img["src"]
            # 如果 URL 以 // 開頭，補上協定
            if image_url.startswith("//"):
                image_url = "https:" + image_url
        else:
            # 若 <noscript> 內未找到，備援使用第一個 img 的 data-thumb 屬性 (根據原始 HTML)
            img = product.select_one("img")
            image_url = img["data-thumb"] if img and img.has_attr("data-thumb") else None
            if image_url and image_url.startswith("//"):
                image_url = "https:" + image_url

        # 判斷是否包含 "売り切れ" 字樣
        text_content = product.get_text()
        status = "売り切れ" if "売り切れ" in text_content else "在庫有"

        return name, image_url, status
    except Exception as e:
        logging.error("解析商品資料錯誤: %s", e)
        return None, None, None

# 依據爬取資料，若商品已存在則更新狀態（若有變動則 notified 設為 0），否則插入新記錄
def upsert_product(cursor, name, image_url, status):
    try:
        select_query = "SELECT id, status FROM products WHERE product_name = %s"
        cursor.execute(select_query, (name,))
        result = cursor.fetchone()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if result:
            product_id, current_status = result
            if current_status != status:
                # 狀態變更，更新狀態、圖片、時間，且重設通知狀態
                update_query = """
                    UPDATE products 
                    SET status = %s, image_url = %s, last_updated = %s, notified = 0 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status, image_url, now, product_id))
                logging.info("更新商品: %s 狀態從 %s 變更為 %s", name, current_status, status)
            else:
                # 若狀態未改變仍更新圖片與更新時間
                update_query = """
                    UPDATE products 
                    SET image_url = %s, last_updated = %s 
                    WHERE id = %s
                """
                cursor.execute(update_query, (image_url, now, product_id))
        else:
            # 新增商品記錄，初始設 notified 為 0
            insert_query = """
                INSERT INTO products (product_name, image_url, status, last_updated, notified) 
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (name, image_url, status, now, 0))
            logging.info("新增商品: %s 狀態: %s", name, status)
    except Error as e:
        logging.error("upsert_product 資料庫操作失敗: %s", e)

# 比對資料庫中已存在但本次爬取中沒有出現的商品，更新狀態為「下架」
def mark_removed_products(cursor, scraped_names):
    try:
        select_query = "SELECT id, product_name, status FROM products"
        cursor.execute(select_query)
        rows = cursor.fetchall()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for row in rows:
            product_id = row[0]
            product_name = row[1]
            current_status = row[2]
            if product_name not in scraped_names and current_status != "下架":
                update_query = """
                    UPDATE products 
                    SET status = %s, last_updated = %s, notified = 0 
                    WHERE id = %s
                """
                cursor.execute(update_query, ("下架", now, product_id))
                logging.info("標記下架商品: %s", product_name)
    except Error as e:
        logging.error("mark_removed_products 資料庫操作失敗: %s", e)

# 主爬取流程：遍歷所有分頁、更新資料庫
def scrape_all():
    connection = get_db_connection()
    if connection is None:
        logging.error("無法取得資料庫連線，爬取中止。")
        return
    cursor = connection.cursor()
    scraped_names = set()  # 紀錄本次爬取到的商品名稱
    page = 1
    while True:
        products = fetch_products(page)
        if not products:
            logging.info("第 %s 頁無商品，結束分頁爬取。", page)
            break
        logging.info("第 %s 頁抓到 %s 個商品。", page, len(products))
        for product in products:
            name, image_url, status = parse_product(product)
            if name is None:
                continue
            scraped_names.add(name)
            upsert_product(cursor, name, image_url, status)
        page += 1
        connection.commit()
    
    # 將資料庫中未出現的商品標記為下架
    mark_removed_products(cursor, scraped_names)
    connection.commit()
    cursor.close()
    connection.close()
    logging.info("爬取作業完成。")

if __name__ == "__main__":
    try:
        scrape_all()
    except Exception as e:
        logging.error("爬蟲程式發生未捕捉錯誤: %s", e)
