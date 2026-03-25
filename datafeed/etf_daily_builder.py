import pandas as pd

from ts_utils import get_etf_daily
import sqlite_utils
from config import Config, DATA_DIR
from datetime import datetime, timedelta

def update_symbol(symbol):
    db = sqlite_utils.Database(DATA_DIR.joinpath('etf_daily.db'))
    table_name = "daily"

    # 1. 判断表是否存在
    if table_name in db.table_names():
        # 2. 获取最大 date（字符串）
        max_date_row = db.execute(f"SELECT MAX(date) FROM {table_name} WHERE symbol='{symbol}'" ).fetchone()
        max_date_str = max_date_row[0] if max_date_row[0] else None
        if max_date_str:
            # 将字符串转为日期，加一天
            max_date = datetime.strptime(max_date_str, "%Y%m%d")
            start_date = max_date + timedelta(days=1)
            start_date_str = start_date.strftime("%Y%m%d")
        else:
            # 表为空，从最早开始
            start_date_str = "20200101"
    else:
        # 表不存在，全量导入
        start_date_str = "20200101"

    print(start_date_str)
    df = get_etf_daily(symbol,start_date=start_date_str)
    print(df)
    db[table_name].insert_all(df.to_dict(orient='records'),pk='id')

def update_all_etf_daily():
    #db = sqlite_utils.Database(DATA_DIR.joinpath('basic.db'))
    df_etf = pd.read_csv(DATA_DIR.joinpath('all_etf.csv'))
    symbols = list(df_etf['基金代码'])

    for s in symbols:
        print(s)
        update_symbol(s)

if __name__ == '__main__':
    update_all_etf_daily()