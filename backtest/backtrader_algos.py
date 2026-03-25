import pandas as pd


class RunDaily:
    def __call__(self, target):
        return True


class RunOnce:
    def __init__(self):
        self.has_run = False

    def __call__(self, target):
        # 如果条件满足且还没有执行过，则返回True并标记为已执行
        if not self.has_run:
            self.has_run = True
            return True
        return False


class RunEveryNPeriods:
    def __init__(self, n, period='days'):
        self.n = n
        self.period = period
        self.last_date = None

    def __call__(self, target):
        current_date = target.datetime.date(0)

        # 如果是第一次调用，记录当前日期并返回True
        if self.last_date is None:
            self.last_date = current_date
            return True

        # 根据不同的时间单位计算间隔
        if self.period == 'days':
            days_diff = (current_date - self.last_date).days
            if days_diff >= self.n:
                self.last_date = current_date
                return True

        elif self.period == 'weeks':
            weeks_diff = ((current_date - self.last_date).days) // 7
            if weeks_diff >= self.n:
                self.last_date = current_date
                return True

        elif self.period == 'months':
            months_diff = (current_date.year - self.last_date.year) * 12 + (current_date.month - self.last_date.month)
            if months_diff >= self.n:
                self.last_date = current_date
                return True

        elif self.period == 'years':
            years_diff = current_date.year - self.last_date.year
            if years_diff >= self.n:
                self.last_date = current_date
                return True

        return False


class RunWeekly:
    def __init__(self):
        self.last_week = None

    def __call__(self, target):
        current_date = target.datetime.date(0)
        current_year, current_week, _ = current_date.isocalendar()
        current_identifier = (current_year, current_week)  # 使用(年,周)组合处理跨年情况

        # 检查周是否变化
        if current_identifier != self.last_week:
            self.last_week = current_identifier
            return True
        return False


class RunMonthly:
    def __init__(self):
        self.last_month = None

    def __call__(self, target):
        current_date = target.datetime.date(0)
        current_year = current_date.year
        current_month = current_date.month
        current_identifier = (current_year, current_month)  # 使用(年,月)组合处理跨年情况

        # 检查月份是否变化
        if current_identifier != self.last_month:
            self.last_month = current_identifier
            return True
        return False


class RunQuarterly:
    def __init__(self):
        self.last_quarter = None

    def __call__(self, target):
        current_date = target.datetime.date(0)
        current_year = current_date.year
        current_month = current_date.month
        current_quarter = (current_month - 1) // 3 + 1  # 计算当前季度
        current_identifier = (current_year, current_quarter)  # 使用(年,季度)组合

        # 检查季度是否变化
        if current_identifier != self.last_quarter:
            self.last_quarter = current_identifier
            return True
        return False


class RunYearly:
    def __init__(self):
        self.last_year = None

    def __call__(self, target):
        current_date = target.datetime.date(0)
        current_year = current_date.year

        # 检查年份是否变化
        if current_year != self.last_year:
            self.last_year = current_year
            return True
        return False




class SelectWhere:
    def __init__(self, signal):
        self.signal = signal

    def __call__(self, target):
        signal = self.signal
        current_date = target.datetime.date(0)  # 获取当前日期

        current_date_pd = pd.Timestamp(current_date)  # 转换为Pandas时间戳

        # 检查当前日期是否在信号索引中
        if current_date_pd in signal.index:
            # 获取当前日期的信号行，并筛选值为True的列
            selected_columns = signal.loc[current_date_pd][signal.loc[current_date_pd]==1].index.to_list()
            target.temp["selected"] = selected_columns

            return True
        else:
            target.temp["selected"] = []
            return True  # 若日期不存在返回空列表

class SelectAll:
    def __call__(self, target):

        symbols = []
        for data in target.datas:
            if len(data) > 0:
                symbols.append(data._name)
        target.temp["selected"] = symbols
        return True

class SelectTopK:
    def __init__(self, signal, K=1, drop_top_n=0, b_ascending=False):
        self.K = K
        self.drop_top_n = drop_top_n  # 这算是一个魔改，就是把最强的N个弃掉，尤其动量指标，过尤不及。
        self.b_ascending = b_ascending
        self.signal = signal

    def __call__(self, target):
        """
            根据当天信号值排序，丢弃N个信号后选取前K个信号

            参数:
            signal: pd.DataFrame - 信号数据框，索引为日期
            N: int - 要丢弃的信号数量
            K: int - 要选取的信号数量
        """
        current_date = target.datetime.date()  # 获取当前日期
        current_date_pd = pd.Timestamp(current_date)  # 转换为Pandas时间戳
        #print(current_date_pd)
        signal =  self.signal

        # 检查当前日期是否在信号索引中
        if current_date_pd not in signal.index:
            target.temp["selected"] = []
            return True  # 若日期不存在返回空列表

        # 获取当前日期的信号行
        daily_signals = signal.loc[current_date_pd]

        # 移除NaN值
        #valid_signals = daily_signals.dropna()
        if 'selected' in target.temp.keys():

            selected = target.temp['selected']
            if selected:
                valid_signals = daily_signals[selected]
            else:
                return True
        else:
            return False


        # 按信号值降序排序
        sorted_signals = valid_signals.sort_values(ascending=self.b_ascending)

        # 丢弃前N个信号（如果需要）
        N = self.drop_top_n
        if N:
            N = int(N)
        else:
            N = 0

        if N > 0:
            # 确保N不超过有效信号数量
            N = min(N, len(sorted_signals))
            sorted_signals = sorted_signals.iloc[N:]

        # 取前K个信号
        K = self.K
        K = int(K)
        if len(sorted_signals) > 0:
            # 确保K不超过剩余信号数量
            K = min(K, len(sorted_signals))
            topK_signals = sorted_signals.head(K)
            target.temp["selected"] = topK_signals.index.tolist()

            return True
        else:
            target.temp["selected"] = []
            return True  # 如果没有剩余信号，返回空列表

class WeightEqually:
    def __init__(self):
        pass

    def __call__(self, target):
        selected = target.temp["selected"]
        n = len(selected)

        if n == 0:
            target.temp["weights"] = {}
        else:
            w = 1.0 / n
            target.temp["weights"] = {x: w for x in selected}
        return True


class WeightFix:
    def __init__(self, weights_dict: dict):
        self.weights_dict = weights_dict
        total_weight = sum(self.weights_dict.values())
        if total_weight > 1:
            raise ValueError(f"权重和不能大于1，当前权重和为：{total_weight}")

    def __call__(self, target):
        print(target.datetime.date())
        target.temp["weights"] = self.weights_dict

class ReBalance:
    def __init__(self, force_rebalance=False):
        # force_rebalance强制调仓，就是说本次调仓，与上次标的完全一样，也会调仓，默认不调仓。
        # 这个适合在 大类资产配置的情形
        self.pre_symbols = None
        self.force_rebalance = force_rebalance

    def __call__(self, target):

        if "weights" not in target.temp:
            return True

        # 这就是目标调仓表，如果没有在调仓表里的，就需要先平仓，把资金腾出来
        target_weights = target.temp["weights"]

        if type(target_weights) is pd.Series:
            target_weights = target_weights.to_dict()

        target.weights = target_weights
        #print('target_weights', target_weights)

        if self.force_rebalance == False and self.pre_symbols == set(target_weights.keys()):
            #print('持仓一样，不调仓')
            return

        #print('当前调仓表', target_weights.keys())
        total_value = target.broker.getvalue()
        date = target.datas[0].datetime.date(0).strftime('%Y-%m-%d')

        # 不存在即平仓
        datas = target.get_current_holding_datas()
        for data in datas:
            if data._name not in target_weights.keys():
                # 平仓
                close_signal = {
                    'symbol': data._name,

                    'pos_from': round(target.getposition(data).size*data.close[0] / total_value,3),
                    'pos_to': 0,
                    'op': '平仓'
                }
                target.signals[date].append(close_signal)

                target.close(data)

        # 存在的按要求调仓
        #logger.debug(target_weights)


        to_buy = []
        for symbol, w in target_weights.items():
            # 卖出的先执行
            pos = target.getposition(target.getdatabyname(symbol))
            close = target.getdatabyname(symbol).close[0]
            data_value = pos.size * close
            percent = data_value / total_value
            #logger.debug('总市值,{},{},{}-size:{},{}'.format(symbol, total_value, data_value, pos.size,close))
            if percent > w:
                #logger.debug('卖出：{}'.format(percent-w))
                sell_signal = {
                    'symbol': symbol,

                    'pos_from':percent,
                    'pos_to': w,
                    'op': '减仓' if w >0 else '平仓'
                }
                target.signals[date].append(sell_signal)
                #target.weights[symbol]=w

                target.order_target_percent(symbol, w)
            else:
                #logger.debug('买入：{}'.format(w - percent))
                to_buy.append(symbol)

        for s in to_buy:
            data = target.getdatabyname(s)
            pos_from = round(target.getposition(target.getdatabyname(s)).size * data.close[0] / total_value, 3)
            buy_signal = {
                'symbol': symbol,

                'pos_from': pos_from,
                'pos_to': w,
                'op': '加仓' if pos_from >0 else '开仓'
            }
            target.signals[date].append(buy_signal)
            #target.weights[symbol] = w
            target.order_target_percent(s, w * 0.99)
        self.pre_symbols = set(target_weights.keys())
        print('当前调仓后的权重',target.weights)
        target.temp = {}

        return True

