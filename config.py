import os
import sys
from typing import Dict

EXCEL_FILE_EXTENSION = (".xlsx", ".xls")
CSV_EXTENSION = (".csv",)
ALL_SUPPORTED_EXTENSIONS = EXCEL_FILE_EXTENSION + CSV_EXTENSION
TARGET_DATABASE = "kayson_db"
DEFAULT_DATABASE = "kayson_db"

def get_project_root() -> str:
    """获取程序根目录（兼容 .py 开发 和 .exe 打包）"""
    if getattr(sys, 'frozen', False):
        # False为打包模式,True为开发模式
        return os.path.dirname(os.path.abspath(sys.executable))
    else:
        return os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT: str = get_project_root()
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "sync.log")

DATA_DIR_TO_DATABASE: Dict[str, str] = {
    "data": "kayson_db",
    "data1": "kayson_db1",
    "data2": "kayson_db2",
    "data3": "kayson_db3",
    "data4": "kayson_db4",
    "data5": "kayson_db5",
    "data6": "kayson_db6",
    "data7": "kayson_db7",

}

if __name__ == "__main__":
    print(f"项目根目录: {PROJECT_ROOT}")
    print(f"日志文件路径: {LOG_FILE}")
    for dir_name in DATA_DIR_TO_DATABASE.keys():
        full_path = os.path.join(PROJECT_ROOT, dir_name)
        exists = os.path.exists(full_path)
        print(f"目录 {dir_name}: {'存在' if exists else '不存在'}")
        if exists:
            files = os.listdir(full_path)
            print(f" 内容：{files[:5]}{'...' if len(files) > 5 else ''}")

DB_CONFIG = {
    "host": "172.17.202.147",
    "port": 3306,
    "user": "sync_user",
    "password": "kayson",
    "charset": "utf8mb4",
}

SYNC_MODE = "replace"
LOG_FILE = "logs/sync.log"
DATE_FORMAT = "%Y/%m/%d"
MONEY_KEYWORDS = {
    'zh': ['金额', '价', '费用', '成本', '收入', '合计', '总额', '税', '折扣', '利润'],
    'en': ['amount', 'price', 'cost', 'fee', 'total', 'sum', 'revenue', 'income', 'discount', 'tax']
}
IGNORE_FILES = ["template.xlsx", "backup.xlsx"]