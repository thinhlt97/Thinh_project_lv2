import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp.client_exceptions import ClientError
from bs4 import BeautifulSoup
import json

# Kết nối MongoDB
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "glamira"
SOURCE_COLLECTION = "tb1"
TARGET_COLLECTION = "product_name"

# Tạo kết nối Motor (MongoDB async client)

client = AsyncIOMotorClient(MONGO_URI)
db = client[DB_NAME]
source_collection = db[SOURCE_COLLECTION]
target_collection = db[TARGET_COLLECTION]

# Update thêm trạng thái status với tất cả record
db[SOURCE_COLLECTION].update_many({}, {"$set": {"status": "pending"}})

# Hàm crawl một URL và retry nếu gặp lỗi
async def fetch_product_name(session, url, product_recordid, retries=3):
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, "html.parser")
                    product_name = None
                    # Try different methods to find the product name
                    methods = [
                        lambda: soup.find('span', class_='base').text,
                        lambda: next((span.text for infor in soup.find_all("div", class_=["info_stone", "info_stone_total"])
                                      for product_item_detail in [soup.find("h2", class_=["product-item-details", "product-name"])]
                                      for span in [product_item_detail.find("span", "hide-on-mobile")]
                                      if json.loads(infor.find("p", class_=["enable-popover", "popover_stone_info"])["data-ajax-data"]).get("product_id") == product_id), None),
                        lambda: soup.find("div", class_="product-info-desc").find("h1").text
                    ]
                    for method in methods:
                        try:
                            product_name = method()
                            if product_name:
                                break
                        except AttributeError:
                            continue
                    return product_name     
                else:
                    print(f"Attempt {attempt + 1} failed for {url}: Status {response.status}")
        except (ClientError, asyncio.TimeoutError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
        
        await asyncio.sleep(2)  # Chờ trước khi retry

    print(f"Failed to fetch after {retries} attempts: {url}")
    return None

# Hàm xử lý một bản ghi
async def process_record(session, record):
    url = record.get("current_url")
    product_id = record.get("product_id")
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    if not url or not product_id:
        print(f"Invalid record: {record}")
        # Cập nhập trạng thái thất bại với record không có đủ cả current_url và product_id
        await source_collection.update_one(
            {"_id": record["_id"]},
            {"$set": {"status": "failed"}}
        )
        return None
   
    await source_collection.update_one(
        {"_id": record["_id"]},
        {"$set": {"status": "in_progress"}}
    )
   
    product_name = await fetch_product_name(session, url,product_id)
    if product_name:
        try:
            # Lưu kết quả vào collection mới
            await target_collection.update_one(
                {"product_id": product_id, "current_url": url},  # Đảm bảo uniqueness dựa trên cặp product_id và url
                {"$set": {"product_name": product_name}},
                upsert=True
            )
         
            # Cập nhật trạng thái là "completed"
            await source_collection.update_one(
                {"_id": record["_id"]},
                {"$set": {"status": "completed"}})
            
            print(f"Saved product_id {product_id} with product_name {product_name}")
        except Exception as e:
            print(f"Error saving to target_collection: {e}")
            
            # Đánh dấu là "failed" nếu gặp lỗi
            await source_collection.update_one(
                {"_id": record["_id"]},
                {"$set": {"status": "failed"}})
            
    else:
        print(f"Failed to fetch product_name for product_id {product_id}")
        
        await source_collection.update_one(
            {"_id": record["_id"]},
            {"$set": {"status": "failed"}})
      

# Hàm chính để xử lý toàn bộ dữ liệu
async def main():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]
    source_collection = db[SOURCE_COLLECTION]
    target_collection = db[TARGET_COLLECTION]
    # Đảm bảo collection target có index unique trên cặp product_id và url
    await target_collection.create_index([("product_id", 1), ("current_url", 1)], unique=True)

    async with aiohttp.ClientSession() as session:
        #cursor = source_collection.find({}, {"_id": 1, "current_url": 1, "product_id": 1})
        
        cursor = source_collection.find(
            {"status": "pending"},
            {"_id": 1, "current_url": 1, "product_id": 1},
            no_cursor_timeout=True
            )
        
        tasks = []
        batch_size = 1000  # Số lượng task chạy đồng thời
        count = 0
        try:
            async for record in cursor:
                tasks.append(asyncio.create_task(process_record(session, record)))
                count += 1

                if len(tasks) >= batch_size:
                    await asyncio.gather(*tasks)
                    tasks = []  # Reset task batch
            if tasks:
                await asyncio.gather(*tasks)
        finally:
            cursor.close()
        # Xử lý các task còn lại

        
        print(f"Processed {count} records.")
if __name__ == "__main__":
    asyncio.run(main())

