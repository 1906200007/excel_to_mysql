
from excel_to_mysql import batch_sync_all_excels
import logging

def daily_batch_sync_job():
    logging.info(" 开始执行每日批量同步任务...")
    batch_sync_all_excels()

if __name__ == "__main__":
    batch_sync_all_excels()

