import time
from excel_to_mysql import sync_all_directories
if __name__ == "__main__":

    sync_all_directories()
    print("✅ 程序执行完毕，将在 60 秒后自动关闭...")
    time.sleep(60)
