import json

#Chia nhỏ file dữ liệu hơn 40 triệu bản ghi ban đầu thành 400 file nhỏ để dễ đẩy từ GCS sang BigQuery
def split_jsonl_file(input_file, output_prefix, num_splits):
    # Tổng số bản ghi hiện tại 
    total_lines = 41432473
    
    # Số dòng mỗi file 
    lines_per_file = total_lines // num_splits
    remainder = total_lines % num_splits  # Số dòng dư 
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for i in range(num_splits):
            output_file = f"{output_prefix}_{i+1}.jsonl"
            lines_to_write = lines_per_file + (1 if i < remainder else 0)  # Thêm dòng dư nếu cần
            with open(output_file, 'w', encoding='utf-8') as out_f:
                for _ in range(lines_to_write):
                    out_f.write(f.readline())

    print(f"File '{input_file}' đã được chia thành {num_splits} file nhỏ hơn.")

input_file = "glamira_full.json"
output_prefix = "small_file"
num_splits = 400

split_jsonl_file(input_file, output_prefix, num_splits)
