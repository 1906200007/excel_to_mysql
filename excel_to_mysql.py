import logging
import pandas as pd
import pymysql
import os
import re
from typing import Optional
from pymysql import cursors
from schedule import clear

from config import (
    DB_CONFIG, DATE_FORMAT, MONEY_KEYWORDS,
    IGNORE_FILES, ALL_SUPPORTED_EXTENSIONS,
    DATA_DIR_TO_DATABASE, PROJECT_ROOT

)

def setup_logging():
    log_dir = os.path.join(PROJECT_ROOT, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "sync.log")
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            handlers=[
                logging.FileHandler(log_path, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )

def normalize_sheet_name(sheet_name: str) -> str:
    """工作表名称规范为MySQL合法表名"""
    #转小写
    name = str(sheet_name).lower()
    #替换空格、连字符为下划线
    name = re.sub(r'[\\/:*?"<>|\s%]', '_' , name)
    #去除连续下划线
    name = re.sub(r'_+', '_', name)
    #去除首尾下划线
    name = name.strip('_')
    #如果工作表名为空则加后缀
    if not name:
        name = "sheet"
    elif name[0].isdigit():
        name = name

    if len(name) > 50:
        name = name[:50].rstrip('_')

    return name

def filename_to_base_table_name(filename: str) -> str:
    """文件名规范为MySQL合法表名，同上"""
    # 1. 去掉扩展名
    base_name = os.path.splitext(filename)[0]

    # 2. 替换 MySQL 标识符中的非法字符为下划线
    #    包括：\ / : * ? " < > | 空格 制表符 换行等
    base_name = re.sub(r'[\\/:*?"<>|\s%]', '_', base_name)

    # 3. 合并多个连续下划线为一个
    base_name = re.sub(r'_+', '_', base_name).strip('_')

    # 4. 处理空或以数字开头的情况
    if not base_name:
        base_name = "表"
    elif base_name[0].isdigit():
        base_name = "表_" + base_name

    # 5. 截断至安全长度（MySQL表名最大64字符，保守截到50字符）
    if len(base_name) > 50:
        base_name = base_name[:50].rstrip('_')

    return base_name

def get_mysql_type(series: pd.Series, col_name: str) -> str:

    _ = col_name

    if pd.api.types.is_datetime64_any_dtype(series):
        return "DATE"

    if series.dtype == 'object':
        max_len = series.astype(str).str.len().max()
        if pd.isna(max_len) or max_len == 0:
            max_len = 255
        else:
            max_len = min(int(max_len * 1.2), 10000)
        return f"VARCHAR({max_len})"

    if pd.api.types.is_integer_dtype(series) or str(series.dtype).startswith("Int"):
        return "BIGINT"

    if pd.api.types.is_float_dtype(series):
        return "DECIMAL(13, 3)"

    return "TEXT"

def is_likely_money_column(col_name: str) -> bool:
    """根据列名判断是否可能是金额列"""
    col_lower = str(col_name).lower()
    # 中文匹配（直接包含）
    for kw in MONEY_KEYWORDS['zh']:
        if kw in col_name:
            return True
    # 英文匹配
    for kw in MONEY_KEYWORDS['en']:
        if kw in col_lower:
            return True
    return False

def preprocess_dataframe(df: pd.DataFrame, source_info: str) -> Optional[pd.DataFrame]:
    """
    预处理可能出现的字段（日期、金额等）
    :param df:
    :param source_info:用于日志
    :return:
    """
    if df.empty:
        logging.warning(f" ！空工作表：{source_info}")
        return None

    df = df.copy()

    #清理列名
    df.columns = [str(col).strip() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    #移除全空行
    df.dropna(how='all', inplace=True)
    if df.empty:
        logging.warning(f" ！移除空行后数据为空：{source_info}")
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
        except Exception as e:
            logging.debug(f"跳过日期解析 '{col}': {e}")
            continue

    for col in df.columns:
        if is_likely_money_column(col):
            try:
                if df[col].dtype == 'object':
                    # 清理非数字字符（保留数字、小数点、负号）
                    cleaned = (
                        df[col].astype(str)
                        .str.replace(r'[^\d.\-]', '', regex=True)
                        .str.replace(r'^(-)?\.$', r'\g<1>0.0', regex=True)  # "-." → "-0.0"
                        .str.replace(r'^(-)?\.(\d)', r'\g<1>0.\2', regex=True)  # ".5" → "0.5", "-.5" → "-0.5"
                        .str.replace(r'\.+', '.', regex=True)
                        .str.replace(r'\.$', '.0', regex=True)  # "123." → "123.0"
                    )
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
                    logging.info(f"自动识别并处理金额列 '{col}' ({source_info})")
                else:
                    # 已是数值型，确保为 numeric（防止 object 类型的数字）
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception as e:
                logging.warning(f"! 金额列 '{col}' 处理异常: {e} ({source_info})")

    #纯整数的浮点列转回整数
    for col in df.columns:
        #检测是否所有非空值都为整数
        if pd.api.types.is_float_dtype(df[col]):
            non_na = df[col].dropna()
            if not non_na.empty and (non_na % 1 == 0).all():
                df[col] = df[col].astype('Int64')
                logging.debug(f" 列 '{col}' 已从 float 转为整数 ({source_info})")
    return df

def create_table_if_not_exists(conn, df: pd.DataFrame, table_name: str):
    """创建自增主键的MySQL数据表"""
    columns_def = ["`索引` BIGINT AUTO_INCREMENT PRIMARY KEY"]

    for col in df.columns:
        mysql_type = get_mysql_type(df[col], col)
        col_def = f"`{col}` {mysql_type}"
        columns_def.append(col_def)

    create_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_def)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    with conn.cursor() as cursor:
        cursor.execute(create_sql)
    conn.commit()
    logging.info(f"✅ 表 `{table_name}` 已创建")


def connect_mysql(database: str):
    config = DB_CONFIG.copy()
    config["database"] = database

    try:
        conn = pymysql.connect(**config, cursorclass=cursors.DictCursor)
        logging.info(f"✅ MySQL 连接数据库 {database} 成功")
        return conn
    except Exception as e:
        logging.error(f"❌ MySQL 连接数据库 {database} 失败：{e}")
        return None


def sync_dataframe_to_table(df: pd.DataFrame, table_name: str, target_database: str) -> bool:
    """文件数据同步到MySQL表"""
    conn = connect_mysql(target_database)
    if not conn or df is None or df.empty:
        return False

    try:
        #清洗列名
        original_columns = df.columns.tolist()
        cleaned_columns = []
        for col in original_columns:
            clean_col = re.sub(r'[\\/:*?"<>|\s%]', '_', str(col))
            clean_col = re.sub(r'_+', '_', clean_col).strip('_')
            if not clean_col:
                clean_col = "column"
            cleaned_columns.append(clean_col)
        df.columns = cleaned_columns

        # 确保表存在（带自增id）

        create_table_if_not_exists(conn, df, table_name)
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")

        cols = [f"`{col}`" for col in df.columns]
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO `{table_name}` ({', '.join(cols)}) VALUES ({placeholders})"

        data = []
        for row in df.values:
            clear_row = [None if pd.isna(val) else val for val in row]
            data.append(tuple(clear_row))

        with conn.cursor() as cursor:
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

def read_and_preprocess_csv(file_path: str, source_info: str) -> Optional[pd.DataFrame]:
    """
    读取并预处理 CSV 文件
    :param file_path:
    :param source_info:
    :return:
    """
    try:
        df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip', dtype=str, keep_default_na=False, na_values=[''])
    except UnicodeDecodeError:
        df = pd.read_csv(file_path, encoding='latin-1', on_bad_lines='skip', dtype=str, keep_default_na=False, na_values=[''])
    return preprocess_dataframe(df, source_info)

def sync_single_file_all_sheets(file_path: str, filename: str, target_database: str) -> int:
    """
    同步单个Excel文件中的所有工作表到独立的MySQL表

    表命名规则：
    - 单工作表：filename -> tablename
    - 多工作表：filename + _ + normalized_sheet_name -> tablename
    """
    logging.info(f" 开始处理文件：{filename} → 数据库: {target_database}")
    base_table_name = filename_to_base_table_name(filename)
    success_count = 0

    try:
        if filename.lower().endswith((".xls", ".xlsx")):
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            if not sheet_names:
                logging.warning(f" !Excel 无工作表：{filename}")
                return 0

            total_sheets = len(sheet_names)

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
                # 生成表名
                if total_sheets == 1:
                    final_name = base_table_name
                else:
                    sheet_part = normalize_sheet_name(sheet_name)
                    final_name = f"{base_table_name}_{sheet_part}"

                if sync_dataframe_to_table(df, final_name, target_database):
                    success_count += 1

        #处理 CSV 文件（单表）
        elif filename.lower().endswith(".csv"):
            df = read_and_preprocess_csv(file_path, filename)
            if df is not None and not df.empty:
                if sync_dataframe_to_table(df, base_table_name, target_database):
                    success_count += 1
            else:
                logging.warning(f" ! CSV 文件为空或无效：{filename}")

    except Exception as e:
        logging.error(f"❌ 处理文件失败：{filename} - {e}", exc_info=True)

    return success_count

def sync_all_directories():
    """自动同步 config 中定义的所有 data 目录到对应数据库"""
    setup_logging()
    logging.info("开始多目录同步任务...")

    total_files_processed = 0
    total_tables_synced = 0

    for dir_name, target_db in DATA_DIR_TO_DATABASE.items():
        full_dir = os.path.join(PROJECT_ROOT, dir_name)
        if not os.path.isdir(full_dir):
            logging.warning(f" 目录不存在，跳过: {full_dir}")
            continue

        files = [
            f for f in os.listdir(full_dir)
            if f.lower().endswith(ALL_SUPPORTED_EXTENSIONS)
            and f not in IGNORE_FILES
            and not f.startswith("~$")
        ]

        if not files:
            logging.info(f" 目录 {dir_name} 下无待处理文件")
            continue

        logging.info(f" 处理目录: {dir_name} → 数据库: {target_db} ({len(files)}) 个文件）")
        tables_in_dir = 0
        for filename in files:
            file_path = os.path.join(full_dir, filename)
            synced = sync_single_file_all_sheets(file_path, filename, target_db)
            tables_in_dir += synced
            total_files_processed += 1

        total_tables_synced += tables_in_dir
        logging.info(f" 目录 {dir_name} 完成：同步 {tables_in_dir} 张表")

    logging.info(f" 全部完成！共处理 {total_files_processed} 个文件，同步 {total_tables_synced} 张表到对应数据库")







