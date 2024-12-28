import pymongo
import IP2Location
import csv

# Kết nối MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["glamira"]
collection = db["tb1"]

# Đường dẫn tới file IP2Location BIN
ip2location = IP2Location.IP2Location('IP-COUNTRY-REGION-CITY.BIN')

# Truy vấn dữ liệu từ MongoDB (chỉ lấy các IP unique)
pipeline = [{"$group": {"_id": "$ip"}}]
ips = [doc["_id"] for doc in collection.aggregate(pipeline)]

# Danh sách lưu kết quả
results = []

# Duyệt qua từng IP và tra cứu quốc gia
for ip in ips:
    try:
        record = ip2location.get_all(ip)
        country_short = record.country_short if record else "Unknown"
        country_name = record.country_long if record else "Unknown"
        region = record.region if record else "Unknown"
        city = record.city if record else "Unknown"
        results.append({"ip": ip, "country_short": country_short,"country_name":country_name,"region":region,"city":city})
    except Exception as e:
        print(f"Error processing IP {ip}: {e}")
        results.append({"ip": ip, "country_short": "Error","country_name":"Error","region":"Error","city":"Error"})

# Xuất kết quả ra file CSV
output_csv = "ip_location_final.csv"
with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=["ip", "country_short","country_name","region","city"])
    writer.writeheader()
    writer.writerows(results)

print(f"Kết quả đã được lưu tại {output_csv}")
