import os

EXCEL_FILE_EXTENSION = (".xlsx", ".xls")
CSV_EXTENSION = (".csv",)
ALL_SUPPORTED_EXTENSIONS = EXCEL_FILE_EXTENSION + CSV_EXTENSION
TARGET_DATABASE = "kayson_db"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
LOG_FILE = os.path.join(PROJECT_ROOT, "sync.log")

if __name__ == "__main__":
    print(f"项目根目录: {PROJECT_ROOT}")
    print(f"data 目录: {DATA_DIR}")
    print(f"data 目录是否存在: {os.path.exists(DATA_DIR)}")
    if os.path.exists(DATA_DIR):
        print(f"data 目录内容：{os.listdir(DATA_DIR)}")

DB_CONFIG = {
    "host": "172.17.202.147",
    "port": 3306,
    "user": "sync_user",
    "password": "kayson",
    "database": TARGET_DATABASE,
    "charset": "utf8mb4",
}

PRIMARY_KEY_COLUMN = "Key"

SYNC_MODE = "replace"
LOG_FILE = "logs/sync.log"
#同步时间
SCHEDULE_TIME = "10:00"

DATE_FORMAT = "%Y/%m/%d"

MONEY_COLUMNS = ["Revenue", "Price", "Amount", "Cost",  "Salary", "Total", "Value"]
IGNORE_FILES = ["template.xlsx", "backup.xlsx"]

