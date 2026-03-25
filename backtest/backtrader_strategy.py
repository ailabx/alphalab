import backtrader as bt
import ffn
import numpy as np
import pandas as pd
from loguru import logger
from collections import defaultdict


class StrategyTemplate(bt.Strategy):



    def __init__(self):
        self.last_month = None
        self.trade_list = []  # 用于存储交易结果
        self.signals = defaultdict(list) # 用于存储策略发出主动调仓指令
        self.weights = defaultdict(str)

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f"{dt.isoformat()}, {txt}")



    def notify_order(self, order):

        """订单状态通知"""
        if order.status in [order.Submitted, order.Accepted]:
            return


        if order.status in [order.Completed]:

            if order.isbuy():
                self.log(f"买入执行, 价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 成本: {order.executed.value:.2f}, 佣金: {order.executed.comm:.2f}")
                self.buy_price = order.executed.price
                self.buy_date = self.datas[0].datetime.date(0)
            elif order.issell():
                profit = order.executed.value - order.created.value
                self.log(f"卖出执行, 价格: {order.executed.price:.2f}, 数量: {order.executed.size}, 收益: {profit:.2f}, 佣金: {order.executed.comm:.2f}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f"订单取消/保证金不足/拒绝")

    def select_all(self):
        return self.datas




    def order_by(self, ind='sorter', topK=1, dropN=0, desc=True):
        # 步骤1：计算所有资产的当前指标值
        ranking = []
        sort_inds = self.inds[ind]
        for d in self.datas:
            # 跳过数据不足的资产
            # if len(d) < self.p.lookback:
            #     continue

            # 获取当前指标值
            if d not in sort_inds.keys():
                print('order_by==>sorter指标不存在，请检查')
                return
            ind_value = sort_inds[d][0]

            # 跳过无效值
            if np.isnan(ind_value):
                continue

            ranking.append((d, ind_value))

        # 按指标值降序排序（从大到小）
        ranking.sort(key=lambda x: x[1], reverse=desc)
        selected = [asset for asset, _ in ranking[dropN:topK]]
        return selected



    def _calculate_erc_weights(self, returns_df):
        """调用ffn计算ERC权重"""
        try:
            # 确保输入是pd.DataFrame
            if not isinstance(returns_df, pd.DataFrame):
                returns_df = pd.DataFrame(returns_df)

            weights = ffn.core.calc_erc_weights(
                returns=returns_df,
                initial_weights=None,  # self.initial_weights,
                covar_method=self.params.covar_method,
                risk_parity_method='ccd',
                maximum_iterations=self.params.max_iter,
                tolerance=self.params.tol
            )
            return weights.values * 0.995
        except Exception as e:
            print(f"计算ERC权重失败: {e}")
            return self.initial_weights  # 失败时使用初始权重

    def _prepare_returns(self, data_selected):
        """收集历史数据并计算收益率"""
        prices = []
        valid_length = True

        # 检查所有资产是否有足够数据
        for i, data in enumerate(data_selected):
            if len(data) < self.params.lookback + 1:
                valid_length = False
                break
            # 获取收盘价（从当前时刻回溯）
            prices.append(data.close.get(size=self.params.lookback + 1))

        if not valid_length:
            return None

        # 转换为DataFrame并计算收益率
        price_df = pd.DataFrame(np.array(prices).T)
        returns = price_df.pct_change().dropna()
        return returns

    def weight_risk_parity(self, data_selected):
        df_returns = self._prepare_returns(data_selected)
        weights = self._calculate_erc_weights(returns_df=df_returns)
        weights = {data:w for data,w in zip(data_selected,weights)}
        return weights

    def weight_equally(self,selected):

        weights = {}
        for data in selected:
            weights[data] = 0.98/len(selected)
        return weights

    def rebalance(self,weights):

        """根据新权重调整仓位"""
        total_value = self.broker.getvalue()
        #print(weights)
        to_buy = {}


        for i, data in enumerate(self.datas):
            if data in weights.keys():
                # 计算目标市值
                target_value = total_value * weights[data]
            else:
                target_value = 0

            # 计算当前持仓市值
            current_value = self.getposition(data).size * data.close[0]

            # 计算需要交易的数量
            size_diff = (target_value - current_value) / data.close[0]

            # 执行订单
            if size_diff > 0:
                to_buy[data] = size_diff

            elif size_diff < 0:
                self.sell(data=data, size=abs(size_diff))

        # self.buy(data=data, size=size_diff)
        for data, size in to_buy.items():
            self.buy(data=data, size=size)

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        logger.info('\n\n%s, %s' % (dt.isoformat(), txt))

    # 取当前的日期
    def get_current_dt(self):
        # print(self.datas[0].datetime)
        dt = self.datas[0].datetime.date(0).strftime('%Y-%m-%d')
        # print(dt)
        return dt

    # 取当前持仓的data列表
    def get_current_holding_datas(self):
        holdings = []
        for data in self.datas:
            if self.getposition(data).size != 0:
                holdings.append(data)
        return holdings

    # 取当前持仓的data列表
    def get_current_holding_symbols(self):
        holdings = []
        for data in self.datas:
            if self.getposition(data).size != 0:
                holdings.append(data._name)
        return holdings

    def get_data_pos_percent(self, name):
        pos = self.getposition(name)
        # print(pos)
        data = self.getdatabyname(name)
        # print(data)
        data_value = self.broker.getvalue([data])
        total_value = self.broker.getvalue()
        percent = data_value / total_value
        return percent

    # 打印订单日志
    def notify_order(self, order):
        return
        # if not self.show_info:
        #     return

        order_status = ['Created', 'Submitted', 'Accepted', 'Partial',
                        'Completed', 'Canceled', 'Expired', 'Margin', 'Rejected']
        # 未被处理的订单
        if order.status in [order.Submitted, order.Accepted]:
            return
            self.log('未处理订单：订单号:%.0f, 标的: %s, 状态状态: %s' % (order.ref,
                                                                         order.data._name,
                                                                         order_status[order.status]))
            return
        # 已经处理的订单
        if order.status in [order.Partial, order.Completed]:
            #return
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, 状态: %s, 订单号:%.0f, 标的: %s, 数量: %.2f, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order_status[order.status],  # 订单状态
                     order.ref,  # 订单编号
                     order.data._name,  # 股票名称
                     order.executed.size,  # 成交量
                     order.executed.price,  # 成交价
                     order.executed.value,  # 成交额
                     order.executed.comm))  # 佣金
            else:  # Sell

                self.log(
                    'SELL EXECUTED, status: %s, ref:%.0f, name: %s, Size: %.2f, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order_status[order.status],
                     order.ref,
                     order.data._name,
                     order.executed.size,
                     order.executed.price,
                     order.executed.value,
                     order.executed.comm))

        elif order.status in [order.Canceled, order.Margin, order.Rejected, order.Expired]:
            # order.Margin资金不足，订单无法成交
            # 订单未完成
            self.log('未完成订单，订单号:%.0f, 标的 : %s, 订单状态: %s' % (
                order.ref, order.data._name, order_status[order.status]))

        self.order = None

    def notify_trade(self, trade):
        # if not self.show_info:
        #return

        #logger.debug('新的交易发生......', trade.status)
        # 交易刚打开时
        if trade.justopened:
            return
            self.log('开仓, 标的: %s, 股数: %.2f,价格: %.2f' % (
                trade.getdataname(), trade.size, trade.price))
        # 交易结束
        elif trade.isclosed:
            data = trade.data
            dtopen = data.num2date(trade.dtopen)
            dtclose = data.num2date(trade.dtclose)

            # 收集交易信息
            trade_info = {
                'symbol': trade.getdataname(),
                #'size': trade.size,
                '买入价': trade.price,
                '卖出价':self.getdatabyname(trade.getdataname()).close[0],
                '盈亏': trade.pnl,
                '盈亏（含佣金）': trade.pnlcomm,
                '佣金': trade.commission,
                '买入日期': dtopen,
                '卖出日期': dtclose,
                '持仓天数': (data.num2date(trade.dtclose) - data.num2date(trade.dtopen)).days,
            }
            self.trade_list.append(trade_info)


            # profit_loss = trade.pnl  # 毛利润
            #
            #
            # # 格式化为字符串
            # open_time_str = dtopen.strftime('%Y%m%d')
            # close_time_str = dtclose.strftime('%Y%m%d')
            #
            # net_profit_loss = trade.pnlcomm  # 净利润（已扣除佣金）
            # # self.log('平仓, 标的: %s, 股数: %.2f,价格: %.2f, GROSS %.2f, NET %.2f, 手续费 %.2f' % (
            # #     trade.getdataname(), trade.size, trade.price, trade.pnl, trade.pnlcomm, trade.commission))
            # print(f'交易{trade.getdataname()}结束, 毛收益: {profit_loss:.2f}, 净收益: {net_profit_loss:.2f}')
            # print(f'开仓时间: {open_time_str}, 平仓时间: {close_time_str}')
            # print(f'开仓价格: {trade.price:.2f}平仓价格：{self.getdatabyname(trade.getdataname()).close[0]}')
            # print(f'交易数量: {trade.size}, 佣金: {trade.commission:.2f}')
        # 更新交易状态
        else:
            self.log('交易更新, 标的: %s, 仓位: %.2f,价格: %.2f' % (
                trade.getdataname(), trade.size, trade.price))


class RotationStrategyTemplate(StrategyTemplate):
    def next(self):
        selected = self.order_by()
        weights = self.weight_equally(selected)
        self.rebalance(weights)


