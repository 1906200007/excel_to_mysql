import logging
import pandas as pd
import pymysql
import os
import re
from typing import List, Optional
from pymysql import cursors
from config import (
    DB_CONFIG, DATE_FORMAT, MONEY_COLUMNS,
    DATA_DIR, IGNORE_FILES, PRIMARY_KEY_COLUMN, ALL_SUPPORTED_EXTENSIONS
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

def get_supported_files() -> List[str]:
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

    supported_files = []
    for file in os.listdir(DATA_DIR):
        if file.startswith("~$"):
            continue
        if file.lower().endswith(ALL_SUPPORTED_EXTENSIONS) and file not in IGNORE_FILES:
            supported_files.append(file)
    return supported_files

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

def get_mysql_type(series: pd.Series, col_name: str) -> str:
    #Key列强制为BIGINT主键
    if col_name == PRIMARY_KEY_COLUMN:
        return "BIGINT"

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
        return "DECIMAL(9, 4)"

    return "TEXT"

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

    #清理列名
    df.columns = [str(col).strip() for col in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]

    #检测主键列存在
    if PRIMARY_KEY_COLUMN not in df.columns:
        logging.error(f"❌ 缺少主键列 '{PRIMARY_KEY_COLUMN}': {source_info}")
        return None

    #移除全空行
    df.dropna(how='all', inplace=True)
    if df.empty:
        logging.warning(f" ！移除空行后数据为空：{source_info}")
        return None

    #移除非主键列全为空的行
    none_key_columns = [col for col in df.columns if col != PRIMARY_KEY_COLUMN]
    if none_key_columns:
        df = df.dropna(subset=none_key_columns, how='all')
        if df.empty:
            logging.warning(f" !所有数据行在非主键列均为空：{source_info}")
            return None

    try:
        df[PRIMARY_KEY_COLUMN] = pd.to_numeric(df[PRIMARY_KEY_COLUMN], errors='coerce')
        df = df.dropna(subset=[PRIMARY_KEY_COLUMN]) #再次移除转换失败的主键
        if df.empty:
            logging.error(f"❌ 主键列无法转化为数字：{source_info}")
            return None

    except Exception as e:
        logging.error(f"❌ 主键列转换失败：{source_info} - {e}")
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

    # 金额列处理
    for col in MONEY_COLUMNS:
            if col in df.columns:
                #清理逗号和货币符号
                if df[col].dtype == 'object':
                    cleaned = df[col].astype(str).str.replace(r'[,$€£¥₹%\s]', '', regex=True)
                    df[col] = pd.to_numeric(cleaned, errors='coerce')
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
                logging.info(f"金额列 '{col}' 已处理 ({source_info})")

    #纯整数的浮点列转回整数
    for col in df.columns:
        if col == PRIMARY_KEY_COLUMN:
            continue
        #检测是否所有非空值都为整数
        if pd.api.types.is_float_dtype(df[col]):
            non_na = df[col].dropna()
            if not non_na.empty and (non_na % 1 == 0).all():
                df[col] = df[col].astype('Int64')
                logging.debug(f" 列 '{col}' 已从 float 转为整数 ({source_info})")

    return df

def create_table_with_key_as_pk(conn, df: pd.DataFrame, table_name: str):
    """创建自增主键的MySQL数据表"""
    columns_def = []

    for col in df.columns:
        mysql_type = get_mysql_type(df[col], col)
        col_def = f"`{col}` {mysql_type}"
        if col == PRIMARY_KEY_COLUMN:
            col_def += " PRIMARY KEY"
        columns_def.append(col_def)

    create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns_def)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

    with conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
        cursor.execute(create_sql)

    conn.commit()
    logging.info(f"✅ 表 `{table_name}` 已重建 (主键: {PRIMARY_KEY_COLUMN})")


def connect_mysql():
    try:
        conn = pymysql.connect(**DB_CONFIG, cursorclass=cursors.DictCursor)
        logging.info("✅ MySQL 连接成功")
        return conn
    except Exception as e:
        logging.error(f"❌ MySQL 连接失败：{e}")
        return None


def sync_dataframe_to_table(df: pd.DataFrame, table_name: str) -> bool:
    """文件数据同步到MySQL表"""
    conn = connect_mysql()
    if not conn or df is None or df.empty:
        return False

    try:
        # 每次重建表（全量覆盖，结构+数据）
        create_table_with_key_as_pk(conn, df, table_name)

        #准备 INSERT
        cols = [f"`{col}`" for col in df.columns]
        placeholders = ",".join(["%s"] * len(cols))
        sql = f"INSERT INTO `{table_name}` ({', '.join(cols)}) VALUES ({placeholders})"

        #转换NaN为None
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

def sync_single_file_all_sheets(file_path: str, filename: str):
    """
    同步单个Excel文件中的所有工作表到独立的MySQL表

    表命名规则：
    - 单工作表：filename -> tablename
    - 多工作表：filename + _ + normalized_sheet_name -> tablename
    """
    logging.info(f" 开始处理文件：{filename}")
    base_table_name = filename_to_base_table_name(filename)
    success_count = 0

    try:
        if filename.lower().endswith((".xls", ".xlsx")):
            excel_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            if not sheet_names:
                logging.warning(f" !Excel 无工作表：{filename}")
                return 0

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
        #处理 CSV 文件（单表）
        elif filename.lower().endswith(".csv"):
            source_info = filename
            df = read_and_preprocess_csv(file_path, source_info)
            if df is not None and not df.empty:
                if sync_dataframe_to_table(df, base_table_name):
                    success_count += 1
            else:
                logging.warning(f" ! CSV 文件为空或无效：{filename}")

    except Exception as e:
        logging.error(f"❌ 处理文件失败：{filename} - {e}", exc_info=True)

    return success_count

def batch_sync_all_files():
    """批量同步所有Excel文件及其所有工作表"""
    setup_logging()
    files = get_supported_files()
    if not files:
        logging.warning(" !data/ 目录下没有找到 .xlsx, .xls, .csv 文件")
        return

    logging.info(f" 发现 {len(files)}个文件：{files}")
    total_success = 0
    total_files = len(files)

    for filename in files:
        filename = str(filename)
        file_path = os.path.join(DATA_DIR, filename)
        success_count = sync_single_file_all_sheets(file_path, filename)
        total_success += success_count

    logging.info(f"✅ 批量同步完成：共处理 {total_files} 个文件，成功同步{total_success} 个工作表")
        
