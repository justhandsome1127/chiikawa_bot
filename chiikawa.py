#!/usr/bin/env python3
import discord
from discord.ext import tasks, commands
import mysql.connector
from mysql.connector import Error
import requests
from bs4 import BeautifulSoup
from io import BytesIO
import logging
import os
import asyncio
from datetime import datetime
from dotenv import load_dotenv

# 讀取 .env 中的 Discord Bot Token
load_dotenv()
DC_KEY = os.getenv("DC_KEY")
logging.basicConfig(
    filename='discord_bot.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

# 設定 Bot 權限
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True

# 使用 commands.Bot 讓我們可以方便使用 tasks 等功能
client = commands.Bot(command_prefix="!", intents=intents)

#########################
# 資料庫連線與共用函式
#########################
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

#########################
# 商品爬蟲相關函式
#########################
def fetch_products(page):
    url = f'https://chiikawamarket.jp/collections/all?page={page}'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error("網頁請求失敗 (Page %s): %s", page, e)
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    # 依照網頁結構，選取每個商品的最外層區塊
    products = soup.select('div.product--root')
    return products

def parse_product(product):
    try:
        # 取得商品名稱
        name_tag = product.select_one("h2.product_name")
        name = name_tag.get_text(strip=True) if name_tag else "未知商品"
        
        # 優先從 <noscript> 裡的 img 取得圖片 URL
        noscript_img = product.select_one("noscript img")
        if noscript_img and noscript_img.has_attr("src"):
            image_url = noscript_img["src"]
            if image_url.startswith("//"):
                image_url = "https:" + image_url
        else:
            # 備援取得第一個 img 的 data-thumb 屬性
            img = product.select_one("img")
            image_url = img["data-thumb"] if img and img.has_attr("data-thumb") else None
            if image_url and image_url.startswith("//"):
                image_url = "https:" + image_url

        # 判斷庫存狀態
        text_content = product.get_text()
        status = "売り切れ" if "売り切れ" in text_content else "在庫有"

        return name, image_url, status
    except Exception as e:
        logging.error("解析商品資料錯誤: %s", e)
        return None, None, None

def upsert_product(cursor, name, image_url, status):
    try:
        select_query = "SELECT id, status FROM products WHERE product_name = %s"
        cursor.execute(select_query, (name,))
        result = cursor.fetchone()
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if result:
            product_id, current_status = result
            if current_status != status:
                update_query = """
                    UPDATE products 
                    SET status = %s, image_url = %s, last_updated = %s, notified = 0 
                    WHERE id = %s
                """
                cursor.execute(update_query, (status, image_url, now, product_id))
                logging.info("更新商品: %s 狀態從 %s 變更為 %s", name, current_status, status)
            else:
                update_query = """
                    UPDATE products 
                    SET image_url = %s, last_updated = %s 
                    WHERE id = %s
                """
                cursor.execute(update_query, (image_url, now, product_id))
        else:
            insert_query = """
                INSERT INTO products (product_name, image_url, status, last_updated, notified) 
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (name, image_url, status, now, 0))
            logging.info("新增商品: %s 狀態: %s", name, status)
    except Error as e:
        logging.error("upsert_product 資料庫操作失敗: %s", e)

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
            # 只更新上次有出現但這次抓不到的商品，若已是「下架」則不動作
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

def scrape_all():
    connection = get_db_connection()
    if connection is None:
        logging.error("無法取得資料庫連線，爬取中止。")
        return
    cursor = connection.cursor()
    scraped_names = set()
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
    mark_removed_products(cursor, scraped_names)
    connection.commit()
    cursor.close()
    connection.close()
    logging.info("爬取作業完成。")

#########################
# Discord Bot 與通知相關函式
#########################
def fetch_unnotified_products(cursor):
    try:
        query = """
            SELECT id, product_name, image_url, status 
            FROM products 
            WHERE notified = 0 AND (status = '売り切れ' OR status = '下架')
        """
        cursor.execute(query)
        return cursor.fetchall()
    except Error as e:
        logging.error("fetch_unnotified_products 資料庫查詢失敗: %s", e)
        return []

def mark_product_notified(cursor, product_id):
    try:
        update_query = "UPDATE products SET notified = 1 WHERE id = %s"
        cursor.execute(update_query, (product_id,))
    except Error as e:
        logging.error("mark_product_notified 資料庫更新失敗: %s", e)

def update_dc_servers():
    """
    檢查 Bot 所在的所有 guild，選擇一個符合條件的文字頻道做為通知頻道
    並更新或插入到 dc_servers 資料表，同時清除資料庫中不再屬於目前 Bot 所在 guild 的紀錄。
    """
    connection = get_db_connection()
    if connection is None:
        logging.error("update_dc_servers: DB connection failed")
        return
    cursor = connection.cursor()
    # 取得目前 Bot 所在的 guild 列表，鍵值為字串形式的 guild.id
    current_guilds = {str(guild.id): guild for guild in client.guilds}
    current_guild_ids = set(current_guilds.keys())
    for guild_id, guild in current_guilds.items():
        chosen_channel = None
        for channel in guild.text_channels:
            # 條件：頻道名稱需含有 "測試"、"test" 或 "bot" (不分大小寫)
            if any(keyword in channel.name.lower() for keyword in ["測試", "test", "bot"]):
                chosen_channel = channel
                break
        # 若找不到滿足條件的頻道，備援選擇第一個文字頻道
        if chosen_channel is None and guild.text_channels:
            chosen_channel = guild.text_channels[0]
        if chosen_channel:
            select_query = "SELECT guild_id FROM dc_servers WHERE guild_id = %s"
            cursor.execute(select_query, (guild_id,))
            result = cursor.fetchone()
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if result:
                update_query = "UPDATE dc_servers SET channel_id = %s, guild_name = %s, updated_at = %s WHERE guild_id = %s"
                cursor.execute(update_query, (str(chosen_channel.id), guild.name, now, guild_id))
            else:
                insert_query = "INSERT INTO dc_servers (guild_id, channel_id, guild_name, updated_at) VALUES (%s, %s, %s, %s)"
                cursor.execute(insert_query, (guild_id, str(chosen_channel.id), guild.name, now))
    # 清除資料庫中已不存在的 guild 記錄
    select_query = "SELECT guild_id FROM dc_servers"
    cursor.execute(select_query)
    records = cursor.fetchall()
    for (record_guild_id,) in records:
        if record_guild_id not in current_guild_ids:
            delete_query = "DELETE FROM dc_servers WHERE guild_id = %s"
            cursor.execute(delete_query, (record_guild_id,))
    connection.commit()
    cursor.close()
    connection.close()

@tasks.loop(hours=1)
async def send_notifications():
    # 先更新商品資料（爬蟲）
    await asyncio.to_thread(scrape_all)
    # 更新 DC Bot 所在伺服器記錄
    await asyncio.to_thread(update_dc_servers)
    connection = get_db_connection()
    if connection is None:
        logging.error("send_notifications: DB connection failed")
        return
    cursor = connection.cursor(dictionary=True)
    products = fetch_unnotified_products(cursor)
    if not products:
        logging.info("send_notifications: No unnotified products.")
    else:
        # 取得 dc_servers 資料，決定每個 guild 要傳訊的頻道
        cursor2 = connection.cursor(dictionary=True)
        cursor2.execute("SELECT guild_id, channel_id FROM dc_servers")
        dc_records = cursor2.fetchall()
        for product in products:
            product_id = product['id']
            name = product['product_name']
            image_url = product['image_url']
            status = product['status']
            message_content = f"商品狀態更新通知：**{name}**\n狀態：{status}"
            for record in dc_records:
                channel_id = int(record['channel_id'])
                channel = client.get_channel(channel_id)
                if channel is None:
                    logging.error(f"Channel with id {channel_id} not found.")
                    continue
                try:
                    if image_url:
                        resp = requests.get(image_url, timeout=10)
                        resp.raise_for_status()
                        img_data = BytesIO(resp.content)
                        discord_file = discord.File(fp=img_data, filename='product.jpg')
                        await channel.send(content=message_content, file=discord_file)
                    else:
                        await channel.send(content=message_content)
                except requests.RequestException as e:
                    logging.error("下載圖片失敗 (%s): %s", name, e)
                except Exception as e:
                    logging.error("Discord 發送通知失敗 (%s) in channel %s: %s", name, channel_id, e)
            mark_product_notified(cursor, product_id)
        connection.commit()
        cursor2.close()
    cursor.close()
    connection.close()
    logging.info("send_notifications task completed.")

#########################
# Bot 事件處理
#########################
@client.event
async def on_ready():
    print(f'目前登入身份：{client.user}')
    # 啟動定時通知任務
    send_notifications.start()

@client.event
async def on_message(message):
    if message.author == client.user:
        return
    if message.content.lower() == 'test':
        await message.channel.send("hi")

client.run(DC_KEY)
