from datetime import datetime

import pandas as pd

from config import DATA_DIR
import sqlite_utils


def load_data(symbols=None, start_date='20200101', end_date=datetime.now().strftime('%Y%m%d')):
    db = sqlite_utils.Database(DATA_DIR.joinpath('daily.db'))
    # 构建SQL查询
    query = "SELECT symbol, date, open, high, low, close, vol FROM bond_daily"
    conditions = []
    params = []

    if symbols is not None:
        placeholders = ','.join(['?' for _ in symbols])
        conditions.append(f"symbol IN ({placeholders})")
        params.extend(symbols)

    conditions.append("date BETWEEN ? AND ?")
    params.extend([start_date, end_date])

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # 执行查询并获取结果
    rows = db.query(query, params)
    df = pd.DataFrame(rows)

    df.rename(columns={'vol':'volume'}, inplace=True)

    # 设置索引
    if not df.empty:
        df = df.set_index(['symbol', 'date'])

    return df

if __name__ == '__main__':
    df = load_data()
    print(df)
    print(len(df))
