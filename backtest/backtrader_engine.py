import importlib
from dataclasses import dataclass, asdict
from typing import List, Dict

import backtrader as bt
import numpy as np
import pandas as pd


from backtrader_strategy import StrategyTemplate
from backtrader_algos import *



from matplotlib import rcParams
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

rcParams['font.family'] = 'SimHei'


@dataclass
class Task:
    name: str = '策略'
    symbols: List[str] = field(default_factory=list)

    start_date: str = '20100101'
    end_date: str = datetime.now().strftime('%Y%m%d')

    benchmark: str = '510300.SH'
    select: str = 'SelectAll'

    select_buy: List[str] = field(default_factory=list)
    buy_at_least_count: int = 0
    select_sell: List[str] = field(default_factory=list)
    sell_at_least_count: int = 1

    order_by_signal: str = ''
    order_by_topK: int = 1
    order_by_dropN: int = 0
    order_by_DESC: bool = True  # 默认从大至小排序

    weight: str = 'WeightEqually'
    weight_fixed: Dict[str, int] = field(default_factory=dict)
    period: str = 'RunDaily'
    period_days: int = None


@dataclass
class StrategyConfig:
    name: str = '策略'
    desc: str = '策略描述'
    config_json: Dict[str, int] = field(default_factory=dict)
    author: str = ''


class AlgoStrategy(StrategyTemplate):
    #params = ('$P1',25)

    def __init__(self, algo_list):
        super(AlgoStrategy, self).__init__()
        self.algos = algo_list

    def prenext(self):
        pass

    def next(self):
        #print(f"next - 当前日期: {self.datetime.date(0)}")
        self.temp = {}

        for algo in self.algos:
            if algo(self) is False:  # 如果algo返回False,直接不运行
                return



from datafeed.csv_dataloader import CsvDataLoader
from datafeed.factor_expr import FactorExpr
class DataFeed:
    def __init__(self, task: Task):
        dfs = CsvDataLoader().read_dfs(symbols=task.symbols,start_date=task.start_date, end_date=task.end_date)

        fields = list(set(task.select_buy + task.select_sell))
        if task.order_by_signal:
            fields += [task.order_by_signal]
        names = fields
        df_all = FactorExpr().calc_formulas(dfs,fields)
        self.df_all = df_all


    def get_factor_df(self, col):
        df_factor = self.df_all.pivot_table(values=col, index=self.df_all.index, columns='symbol')
        if col == 'close':
            df_factor = df_factor.ffill()
        return df_factor


class Engine:
    def __init__(self, dfs_dict: dict[str,pd.DataFrame], commission=0.0001):
        self.dfs_dict = dfs_dict
        self.commission = commission
        self._init_engine()


    def _parse_rules(self, task: Task):

        def _rules(rules, at_least):
            if not rules or len(rules) == 0:
                return None

            all = None
            for r in rules:
                if r == '':
                    continue

                df_r = self.datafeed.get_factor_df(r)
                if df_r is not None:
                    df_r = df_r.replace({True: 1, False: 0})
                    df_r = df_r.astype('Int64')

                    #print(df_r)
                    #df_r = df_r.astype(int)
                #else:
                    #print(r)
                if all is None:
                    all = df_r
                else:
                    all += df_r
            return all >= at_least

        buy_at_least_count = task.buy_at_least_count
        if buy_at_least_count <= 0:
            buy_at_least_count = len(task.select_buy)

        all_buy = _rules(task.select_buy, at_least=buy_at_least_count)
        all_sell = _rules(task.select_sell, task.sell_at_least_count)  # 卖出 求或，满足一个即卖出

        if all_sell is not None:
            all_sell = all_sell.fillna(True)
        if all_buy is not None:
            all_buy = all_buy.fillna(False)
        return all_buy, all_sell

    def _get_algos(self, task: Task):

        bt_algos = importlib.import_module('backtrader_algos')

        if task.period == 'RunEveryNPeriods':
            algo_period = bt.algos.RunEveryNPeriods(n=task.period_days, run_on_last_date=True)
        else:
            algo_period = getattr(bt_algos,task.period)()

        algo_select_where = None
        # 信号规则
        signal_buy, signal_sell = self._parse_rules(task)
        if signal_buy is not None or signal_sell is not None:  # 至少一个不为None
            df_close = self.datafeed.get_factor_df('close')
            if signal_buy is None:
                select_signal = np.ones(df_close.shape)  # 注意这里是全1，没有select_buy就是全选
                select_signal = pd.DataFrame(select_signal, columns=df_close.columns, index=df_close.index)
            else:
                select_signal = np.where(signal_buy, 1, np.nan)  # 有select_buy的话，就是买入，否则选置Nan表示 hold状态不变
            if signal_sell is not None:
                select_signal = np.where(signal_sell, 0, select_signal)  # select_sell置为0，就是清仓或不选
            select_signal = pd.DataFrame(select_signal, index=df_close.index, columns=df_close.columns)
            select_signal.ffill(inplace=True)  # 这一句非常关键，ffill就是前向填充，保持持仓状态不变。即不符合buy，也不符合sell，保持不变。
            select_signal.fillna(0, inplace=True)
            algo_select_where = SelectWhere(signal=select_signal)

        # 排序因子
        algo_order_by = None
        if task.order_by_signal:
            signal_order_by = self.datafeed.get_factor_df(col=task.order_by_signal)
            algo_order_by = SelectTopK(signal=signal_order_by, K=task.order_by_topK, drop_top_n=task.order_by_dropN,
                                       b_ascending=task.order_by_DESC==False)

        algos = []
        algos.append(algo_period)

        if algo_select_where:
            algos.append(algo_select_where)
        else:
            algos.append(SelectAll())

        if algo_order_by:
            algos.append(algo_order_by)


        algo_weight = WeightEqually()
        if task.weight == 'WeightFix':
            algo_weight =  WeightFix(weights_dict=task.weight_fixed)

        algos.append(algo_weight)

        force_update=False
        if task.weight == 'WeightFix':
            force_update = True
        algos.append(ReBalance(force_update))
        return algos


    def _init_engine(self):
        cerebro = bt.Cerebro()
        cerebro.broker.setcash(1000000.0)  # 设置初始资金
        cerebro.broker.setcommission(self.comission)
        # 添加PyFolio分析器
        cerebro.addanalyzer(bt.analyzers.PyFolio, _name='_PyFolio')

        # cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        # cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
        # cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='timereturn')
        cerebro.broker.set_coc(True)  # 设置Cheat-On-Close，确保在收盘时执行订单
        self.cerebro = cerebro

    def _prepare_run(self, symbols, start_date, end_date, commissions=0.0):



        start_date = '20100101'
        for s, data in self.dfs_dict.items():
            data['openinterest'] = 0
            data.set_index('date', inplace=True)
            if  data.index[0] > start_date:
                start_date = data.index[0]

            data.index = pd.to_datetime(data.index)
            data.sort_index(ascending=True, inplace=True)

            data = data[
                (data.index >= pd.to_datetime(start_date)) &
                (data.index <= pd.to_datetime(end_date))
                ]

            data = bt.feeds.PandasData(
                dataname=data,
                fromdate=pd.to_datetime(start_date),
                todate=pd.to_datetime(end_date),
                timeframe=bt.TimeFrame.Days,
                name=s,
            )
            self.cerebro.adddata(data)


    def run_strategy(self, strategy, symbols,start_date='20101001', end_date=datetime.now().strftime('%Y%m%d'),*args,**kwargs):
        self._prepare_run(symbols,start_date, end_date)
        self.cerebro.addstrategy(strategy,*args,**kwargs)
        self.results = self.cerebro.run()
        portfolio_stats = self.results[0].analyzers.getbyname('_PyFolio')
        returns, positions, transactions, _ = portfolio_stats.get_pf_items()
        returns.index = returns.index.tz_convert(None)

        #equity = (1 + returns).cumprod()
        self.perf = (1 + returns).cumprod().calc_stats()
        # equity.plot()
        # import matplotlib.pyplot as plt
        # plt.show()


    def run(self, task: Task, commissions=0.0):
        task.end_date = datetime.now().strftime('%Y%m%d')
        self._prepare_run(task.symbols,task.start_date,task.end_date, commissions)
        self.datafeed = DataFeed(task)

        self.cerebro.addstrategy(AlgoStrategy, algo_list=self._get_algos(task))
        self.results = self.cerebro.run()

        timereturn = self.results[0].analyzers.timereturn.get_analysis()
        returns_series = pd.Series(timereturn)
        #print('TimeReturn 分析器结果:', returns_series)

        portfolio_stats = self.results[0].analyzers.getbyname('_PyFolio')
        returns, self.positions, self.transactions, _ = portfolio_stats.get_pf_items()
        #print('returns', returns)

        all_datas = self.cerebro.datas

        # 获取总资产（现金 + 所有持仓市值）
        total_value = self.cerebro.broker.getvalue()

        # print("所有Datas的最新仓位:")
        # print(f"总资产: {total_value:.2f} (现金 + 持仓市值)")

        # 计算所有持仓的总市值
        # total_position_value = 0
        # positions_info = []
        #
        # for data in all_datas:
        #     position = self.cerebro.broker.getposition(data)
        #     if position.size != 0:  # 只显示有持仓的
        #         # 计算当前持仓市值
        #         current_price = data.close[0]
        #         position_value = position.size * current_price
        #         total_position_value += position_value
        #
        #         positions_info.append({
        #             'data': data,
        #             'position': position,
        #             'position_value': position_value
        #         })
        #
        # # 计算现金
        # cash = self.cerebro.broker.getcash()
        # print(f"现金: {cash:.2f}")
        # print(f"持仓总市值: {total_position_value:.2f}")
        # print(f"总资产验证: {cash + total_position_value:.2f} (应与上面总资产一致)")

        # # print("\n各持仓仓位比例:")
        # self.weights = {}
        # for info in positions_info:
        #     data = info['data']
        #     position = info['position']
        #     position_value = info['position_value']
        #
        #     # 计算仓位比例
        #     position_ratio = position_value / total_value
        #     self.weights[data._name] = round(position_ratio,3)


            # print(f"  {data._name}:")
            # print(f"    持仓数量: {position.size}")
            # print(f"    平均成本: {position.price:.2f}")
            # print(f"    当前价格: {data.close[0]:.2f}")
            # print(f"    持仓市值: {position_value:.2f}")
            # print(f"    仓位比例: {position_ratio:.4f} ({position_ratio * 100:.2f}%)")

        #print(self.weights)
        #print(positions, transactions)
        returns.index = returns.index.tz_convert(None)
        # import empyrical
        # ret = empyrical.annual_return(returns)
        # print('年化收益',ret)

        returns.name = '策略'


        equity = pd.DataFrame((1 + returns).cumprod())
        print('equity起始日期', equity.index[0])
        import ffn
        #print('长度：',len(equity))
        #equity.calc_stats().display()
        datas = [equity]
        for bench in [task.benchmark]:
            df = CsvDataLoader().read_df([bench],start_date=task.start_date, end_date=task.end_date)
            df.set_index('date',inplace=True)
            df.index = pd.to_datetime(df.index)
            data = df.pivot_table(values='close', index=df.index, columns='symbol')
            data.columns = ['benchmark']
            datas.append(data)


        all_returns = pd.concat(datas, axis=1).pct_change()
        all_returns.dropna(inplace=True)

        #print('起始日期',all_returns.index[0])

        self.perf = (1 + all_returns).cumprod().calc_stats()
        self.hist_trades =self.results[0].trade_list
        self.hist_trades.reverse()
        self.signals = self.results[0].signals
        self.weights = self.results[0].weights
        print(self.weights)

        #print(self.hist_trades)


        #print(pd.DataFrame(self.results[0].trade_list))
        self.stats()
        return self.results

    def opt(self, strategy,symbols,start_date='20101001', end_date=datetime.now().strftime('%Y%m%d'),*args,**kwargs):
        self._prepare_run(symbols, start_date, end_date)
        self.cerebro.optstrategy(
            strategy,
            period=[5,10,15,20,25,30]
        )

        # 打印结果
        def get_my_analyzer(result):
            analyzer = {}
            # 返回参数
            analyzer['period'] = result.params.period
            #analyzer['period2'] = result.params.lower

            pyfolio = result.analyzers.getbyname('_PyFolio')
            returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
            #print(positions, transactions)
            import empyrical as em
            analyzer['年化收益率'] = em.annual_return(returns)
            return analyzer

        self.results = self.cerebro.run(stdstats=False)
        ret = []
        for i in self.results:
            #print(i)
            ret.append(get_my_analyzer(i[0]))

        df = pd.DataFrame(ret)
        #print(df)



    def stats(self):

        print(self.perf.display())

    def plot(self):
        self.perf.plot()
        import matplotlib.pyplot as plt
        # 设置字体为 SimHei（黑体）
        plt.rcParams['font.sans-serif'] = ['SimHei']
        # 解决坐标轴负号显示问题
        plt.rcParams['axes.unicode_minus'] = False
        plt.show()



if __name__ == '__main__':
    Engine()