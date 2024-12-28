Định nghĩa các file: 
Process_ip.py để chuyển dữ liệu địa chỉ ip trong data gốc kết hợp với file Ip2location.bin để ra được thông tin về quốc gia, thành phố của địa chỉ ip đó
Crawl_product_name.py để lấy được product_name của sản phẩm dựa theo product_id và current_url
Split_data_raw.py để chia dữ liệu hơn 40 triệu bản ghi ban đầu thành các bản ghi bé hơn đẩy lên GCSGCS
cloud_function_upload_gcs_to_bq : Folder chứa file main.py để định nghĩa cho function trên cloud Function, file requirements.txt chứa các thư viện cần thiết.
dbt_glamira_final: Folder chứa các câu lệnh để tạo bảng bằng dbt

Thứ tự luồng dữ liệu
- Chạy file crawl_product_name.py để lấy được product_name của sản phẩm -> Xuất ra file glamira_product_name.csv
- Chạy file process_ip.py lấy được tên quốc gia, thành phố theo địa chỉ ip -> Xuất ra file 
