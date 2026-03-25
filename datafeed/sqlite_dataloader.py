from datetime import datetime

import pandas as pd

from config import DATA_DIR
import sqlite_utils


def load_data(symbols=None, start_date='20200101', end_date=datetime.now().strftime('%Y%m%d')):
    db = sqlite_utils.Database(DATA_DIR.joinpath('daily.db'))
    # 构建SQL查询
    query = "SELECT symbol, date, open, high, low, close, vol,cb_over_rate FROM bond_daily"
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
    query += " ORDER BY date ASC"  # 或 ORDER BY date ASC/DESC
    # 执行查询并获取结果
    rows = db.query(query, params)
    df = pd.DataFrame(rows)

    df.rename(columns={'vol':'volume'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])

    # 设置索引
    if not df.empty:
        df = df.set_index(['date', 'symbol'])

    return df

if __name__ == '__main__':
    df = load_data()
    # 假设原始 DataFrame 名为 df，索引为 MultiIndex，第一级是 date，第二级是 symbol
    dfs_dict = {symbol: group.droplevel('symbol') for symbol, group in df.groupby(level='symbol')}
    print(df)
    print(len(df))
    print(dfs_dict)
