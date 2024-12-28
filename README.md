Định nghĩa các file: 
1. Process_ip.py: để chuyển dữ liệu địa chỉ ip trong data gốc kết hợp với file Ip2location.bin để ra được thông tin về quốc gia, thành phố của địa chỉ ip đó
2. Crawl_product_name.py: để lấy được product_name của sản phẩm dựa theo product_id và current_url
3. Split_data_raw.py: để chia dữ liệu hơn 40 triệu bản ghi ban đầu thành các bản ghi bé hơn đẩy lên GCSGCS
4. cloud_function_upload_gcs_to_bq: Folder chứa file main.py để định nghĩa cho function trên cloud Function, file requirements.txt chứa các thư viện cần thiết.
5. dbt_glamira_final: Folder chứa các câu lệnh để tạo bảng bằng dbt
6. glamira_product_name.csv: danh sách tên của sản phẩm kèm product id
7. ip_location_final.csv: danh sách địa chỉ ip kèm quốc gia, thành phố tương ứng
Thứ tự luồng dữ liệu
- Chạy file crawl_product_name.py để lấy được product_name của sản phẩm -> Xuất ra file glamira_product_name.csv
- Chạy file process_ip.py lấy được tên quốc gia, thành phố theo địa chỉ ip -> Xuất ra file ip_location_final.csv
- Chạy file split_data_raw.py, tách ra được 400 file nhỏ hơn
- Tạo Bucket trên GCS
- Tạo Cloud Function, sử dụng file main.py và requirements.txt để có thể chuyển dữ liệu được upload từ gcs sang bigquery
- Tạo Dataset trên Bigquery
- Upload các file lên GCS
- Sử dụng dbt để tạo các bảng
- Visualize bằng looker
