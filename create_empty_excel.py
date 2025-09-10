
import openpyxl
import os

def create_empty_excel(file_path):
    workbook = openpyxl.Workbook()
    workbook.save(file_path)

if __name__ == "__main__":
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    files_to_create = [
        "video_data.xlsx",
        "live_data.xlsx",
        "msg_data.xlsx",
        "account_bi.xlsx",
        "leads.xlsx",
        "spending.xlsx",
        "account_base.xlsx",
    ]

    for filename in files_to_create:
        file_path = os.path.join(data_dir, filename)
        create_empty_excel(file_path)
        print(f"Created empty Excel file: {file_path}")
