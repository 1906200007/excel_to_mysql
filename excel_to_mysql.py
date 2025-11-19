import logging

import pandas as pd
import pymysql
import os
import re
from pymysql import cursors
from config import (
    DB_CONFIG, SYNC_MODE, DATE_FORMAT, MONEY_COLUMNS,
    DATA_DIR, EXCEL_FILE_EXTENSION, IGNORE_FIELDS
)

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("logs/sync.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

def get_excel_files():
    """获取data目录下的Excel文件"""
    print(f" 查找Excel文件...")
    print(f" DATA_DIR: '{DATA_DIR}'")
    print(f" 绝地路径 = '{os.path.abspath(DATA_DIR)}'")

    if not os.path.exists(DATA_DIR):
        print(f" ERROR：data目录不存在！")
        print(f" 当前工作目录：{os.getcwd()}")
        print(f" 确保data文件夹在位置：{os.path.abspath(DATA_DIR)}")
        return []

    all_files = os.listdir(DATA_DIR)
    print(f" data 目录中的所有文件：{all_files}")

    excel_files = []
    for file in all_files:
        if file.lower().endswith(EXCEL_FILE_EXTENSION) and file not in IGNORE_FIELDS:
            excel_files.append(file)

    print(f" 找到 {len(excel_files)} 个 Excel 文件： {excel_files}")
    return excel_files

def normalize_sheet_name(sheet_name: str) -> str:
    """工作表名称规范为MySQL合法表名"""
    #转小写
    name = str(sheet_name).lower()
    #替换空格、连字符为下划线
    name = re.sub(r'[^a-zA-Z0-9]', '_' , name)
    #去除连续下划线
    name = re.sub(r'_+', '_', name)
    #去除首尾下划线
    name = name.strip('_')
    #如果工作表名为空则加后缀
    if not name or name[0].isdigit():
        name = "sheet_" + name if name else "sheet"
    return name

def filename_to_base_table_name(filename: str) -> str:
    """文件名规范为MySQL合法表名，同上"""
    base_name = os.path.splitext(filename)[0].lower()
    base_name = re.sub(r'[^a-zA-Z0-9]', '_' , base_name)
    base_name = re.sub(r'_+', '_', base_name).strip('_')
    if not base_name or base_name[0].isdigit():
        base_name = "table_" + base_name if base_name else "table"
    return base_name

def preprocess_dataframe(df: pd.DataFrame, source_info: str) -> pd.DataFrame | None:
    """
    预处理可能出现的字段（日期、金额等）
    :param df:
    :param source_info:用于日志
    :return:
    """
    if df.empty:
        logging.warning(f" ！空工作表：{source_info}")
        return None

    #日期列自动识别
    for col in df.select_dtypes(include=['object']).columns:
        sample = df[col].dropna().head(10)
        if len(sample) == 0:
            continue
        try:
            #尝试使用统一格式解析
            parsed = pd.to_datetime(sample, format=DATE_FORMAT, errors="coerce")
            if parsed.notna().mean() > 0.5:
                df[col] = pd.to_datetime(df[col], format=DATE_FORMAT, errors="coerce")
                logging.info(f"日期列 '{col}' 已转换 ({source_info})")
        except:
            continue

    # 金额列处理
    for col in MONEY_COLUMNS:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                logging.info(f"金额列 '{col}' 已处理 ({source_info})")

    #清理全空行
    df = df.dropna(how='all')
    logging.info(f"✅ Excel 预处理完成: {source_info} -> {len(df)} 行, {len(df.columns)} 列")
    return df

def get_mysql_type(series: pd.Series, col_name: str) -> str:
    """根据列类型返回MySQL类型"""
    dtype = str(series.dtype)

    if col_name in MONEY_COLUMNS:
        return "DECIMAL(12,2)"

    if dtype.startswith('datetime'):
        return 'DATE'

    elif dtype in ('int64', 'Int64'):
        return 'BIGINT'
    elif dtype == 'float64':
        return 'DOUBLE'

    elif dtype == 'bool':
        return 'BOOLEAN'

    elif dtype == 'object':
        try:
            max_len = int(series.astype(str).str.len().max())
            if pd.isna(max_len):
                max_len = 255
            return f'VARCHAR({min(max_len + 50, 255)})' if max_len <= 200 else 'TEXT'
        except:
            return 'TEXT'

    else:
        return 'TEXT'

def create_table_with_auto_increment(conn, df: pd.DataFrame, table_name: str):
    """创建自增主键的MySQL数据表"""
    columns_def = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]

    for col in df.columns:
        mysql_type = get_mysql_type(df[col], col)
        columns_def.append(f"`{col}` {mysql_type}")

    create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_def)});"

    with conn.cursor() as cursor:
        cursor.execute(create_sql)
    conn.commit()
    logging.info(f"✅ 表 `{table_name}` 已创建（带自增主键）")


def connect_mysql():
    try:
        conn = pymysql.connect(**DB_CONFIG, cursorclass=cursors.DictCursor)
        logging.info("✅ MySQL 连接成功")
        return conn
    except Exception as e:
        logging.error(f"❌ MySQL 连接失败：{e}")
        return None


def sync_dataframe_to_table(df: pd.DataFrame, table_name: str):
    """文件数据同步到MySQL表"""
    conn = connect_mysql()
    if not conn or df is None or df.empty:
        return False

    try:
        #创建自增主键表
        create_table_with_auto_increment(conn, df, table_name)

        #准备数据，处理NaN
        cols = [f"`{col}`" for col in df.columns]
        placeholders = ",".join(["%s"] * len(cols))
        sql = f"INSERT INTO `{table_name}` ({', '.join(cols)}) VALUES ({placeholders})"

        #转换NaN为None
        data = []
        for row in df.values:
            clear_row = [None if pd.isna(val) else val for val in row]
            data.append(tuple(clear_row))

        with conn.cursor() as cursor:
            if SYNC_MODE == "replace":
                cursor.execute(f"DELETE FROM `{table_name}`")

            cursor.executemany(sql, data)

        conn.commit()
        logging.info(f"✅ 同步成功：表 `{table_name}` ← {len(data)} 行")
        return True

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ 同步失败：`{table_name}` - {e}", exc_info=True)
        return False
    finally:
        conn.close()

def sync_single_excel_all_sheets(file_path: str, filename: str):
    """
    同步单个Excel文件中的所有工作表到独立的MySQL表

    表命名规则：
    - 单工作表：filename -> tablename
    - 多工作表：filename + _ + normalized_sheet_name -> tablename
    """
    logging.info(f" 开始处理文件：{filename}")

    try:
        excel_file = pd.ExcelFile(file_path, engine='openpyxl')
        sheet_names = excel_file.sheet_names

        if not sheet_names:
            logging.warning(f" 文件无工作表：{filename}")
            return 0

        logging.info(f" 发现 {len(sheet_names)} 个工作表：{sheet_names}")
        base_table_name = filename_to_base_table_name(filename)
        success_count = 0

        #遍历每个工作表
        for sheet_name in sheet_names:
            source_info = f"{filename}/{sheet_name}"

            #读取工作表数据
            try:
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            except Exception as e:
                logging.error(f"❌ 读取工作表失败：{source_info} - {e}")
                continue

            #预处理数据
            df = preprocess_dataframe(df, source_info)
            if df is None or df.empty:
                continue

            #生成表名
            if len(sheet_names) == 1:
                #单工作表：直接使用文件名作为表名
                final_table_name = base_table_name
            else:
                #多工作表：文件名_工作表名
                normalize_sheet = normalize_sheet_name(sheet_name)
                final_table_name = f"{base_table_name}_{normalize_sheet}"

            if sync_dataframe_to_table(df, final_table_name):
                success_count += 1

        return success_count

    except Exception as e:
        logging.error(f"❌ 处理文件失败：{filename} - {e}", exc_info=True)
        return 0

def batch_sync_all_excels():
    """批量同步所有Excel文件及其所有工作表"""
    setup_logging()
    excel_files = get_excel_files()
    if not excel_files:
        logging.warning("data/ 目录下没有找到 Excel 文件")
        return

    logging.info(f"发现 {len(excel_files)}个 Excel 文件：{excel_files}")

    total_success = 0
    total_files = len(excel_files)

    for filename in excel_files:
        file_path = os.path.join(DATA_DIR, filename)
        success_count = sync_single_excel_all_sheets(file_path, filename)
        total_success += success_count

    logging.info(f"✅ 批量同步完成：共处理 {total_success} 个工作表")
        
