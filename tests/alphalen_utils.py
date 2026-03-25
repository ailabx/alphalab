import numpy as np

from alphalens.utils import get_clean_factor_and_forward_returns
from alphalens.tears import create_summary_tear_sheet
from datafeed.sqlite_dataloader import load_data
from panda_factor.generate.factor_engine import FactorEngine

df = load_data()
df['close'] = df.groupby(level='date')['close'].transform(lambda x: (x - x.mean()) / x.std())
df['cb_over_rate'] = df.groupby(level='date')['cb_over_rate'].transform(lambda x: (x - x.mean()) / x.std())

prices = df['close'].unstack(level='symbol')  # 或 level=1 根据索引顺序
prices = prices.dropna(axis=1, how='all')  # 删除全为 NaN 的列


print(prices)


# 创建引擎实例
factor_engine = FactorEngine(safe_mode=True)

factor = factor_engine.calc_formula(df, 'CLOSE+cb_over_rate')
factor = factor.dropna()
print(factor)
clean = get_clean_factor_and_forward_returns(factor=factor, prices=prices,periods=[1,5,10,21])
clean = clean.replace([np.inf, -np.inf], np.nan).dropna()
# 再次检查
# print("clean 形状:", clean.shape)
# print("缺失值数量:", clean.isnull().sum().sum())
# print("inf 数量:", np.isinf(clean).sum().sum())
# print("哪些列有 inf:\n", clean.isin([np.inf, -np.inf]).sum())

clean.dropna()
create_summary_tear_sheet(clean)