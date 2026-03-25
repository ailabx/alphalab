# 导入tushare
import tushare as ts
from config import Config

# 初始化pro接口
pro = ts.pro_api(Config.TS_TOKEN)

def get_bond_list():
    # 拉取数据
    df = pro.cb_basic(**{
        "ts_code": "",
        "list_date": "",
        "exchange": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        #"bond_full_name",
        "bond_short_name",
        #"cb_code",
        "stk_code",
        "stk_short_name",
        "maturity",
        "maturity_date",
        "list_date",
        "delist_date",
        "exchange",
        "conv_start_date",
        "conv_end_date",
        "conv_stop_date",
        "first_conv_price",
        "conv_price"
    ])

    df = df[df['list_date'] >= '20200101']
    df.rename(columns={'ts_code': 'symbol'}, inplace=True)
    return df

def get_bond_daily(symbol,start_date='20100101'):
    # 拉取数据
    df = pro.cb_daily(**{
        "ts_code": symbol,
        "trade_date": "",
        "start_date": start_date,
        "end_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
        "bond_value",
        "bond_over_rate",
        "cb_value",
        "cb_over_rate"
    ])
    df.rename(columns={'trade_date':'date', 'ts_code': 'symbol'}, inplace=True)
    df['id'] =  df['symbol'] + '_' + df['date']
    return df


if __name__ == '__main__':
    # df = get_bond_list()
    # print(df)
    # import sqlite_utils
    # from config import DATA_DIR
    #
    # db = sqlite_utils.Database(DATA_DIR.joinpath("basic.db"))
    # # This line creates a "dogs" table if one does not already exist:
    # if 'bond' in db.tables:
    #     db['bond'].drop()
    # db["bond"].insert_all(df.to_dict(orient='records'), pk="symbol")
    # print(db['bond'].count)

    df = get_bond_daily('128082.SZ')
    print(df)