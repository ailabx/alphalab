import pandas as pd
import numpy as np
from typing import Tuple


class FactorUtils:
    """Factor calculation utility class, provides all common calculation methods"""

    @staticmethod
    def RANK(series: pd.Series) -> pd.Series:
        """Cross-sectional ranking, normalized to [-0.5, 0.5] range"""

        def rank_group(group):
            valid_data = group.dropna()
            if len(valid_data) == 0:
                return pd.Series(0, index=group.index)
            ranks = valid_data.rank(method='average')
            ranks = (ranks - 1) / (len(valid_data) - 1) - 0.5
            result = pd.Series(index=group.index)
            result.loc[valid_data.index] = ranks
            result.fillna(0, inplace=True)
            return result

        # Ensure correct index
        if not isinstance(series.index, pd.MultiIndex):
            series.index = pd.MultiIndex.from_tuples(
                [(d, s) for d, s in zip(series.index, series.index)],
                names=['date', 'symbol']
            )
        elif series.index.names != ['date', 'symbol']:
            series.index.names = ['date', 'symbol']

        # Calculate ranking by date group
        result = series.groupby(level='date', group_keys=False).apply(rank_group)

        # Ensure result index is correct
        if isinstance(result.index, pd.MultiIndex):
            if len(result.index.names) > 2:
                result = result.droplevel(0)
            if result.index.names != ['date', 'symbol']:
                result.index.names = ['date', 'symbol']

        return result

    @staticmethod
    def RETURNS(close: pd.Series, period: int = 1) -> pd.Series:
        """Calculate returns

        Args:
            close: Price series
            period: Return calculation period, default is 1

        Returns:
            pd.Series: Return series
        """

        def calculate_returns(group):
            group = group.sort_index(level='date')
            result = group.pct_change(periods=period)
            result.iloc[:period] = 0
            return result

        result = close.groupby(level='symbol', group_keys=False).apply(calculate_returns)
        return result

    @staticmethod
    def FUTURE_RETURNS(close: pd.Series, period: int = 1) -> pd.Series:
        """Calculate future returns

        Args:
            close: Price series
            period: Future periods to calculate returns for, default is 1

        Returns:
            pd.Series: Future return series
        """

        def calculate_future_returns(group):
            group = group.sort_index(level='date')
            # Shift prices backward to calculate future returns
            shifted_prices = group.shift(-period)
            # Calculate percentage change from current to future price
            result = (shifted_prices - group) / group
            # Set the last 'period' values to 0 as they have no future data
            result.iloc[-period:] = 0
            return result

        result = close.groupby(level='symbol', group_keys=False).apply(calculate_future_returns)
        return result

    @staticmethod
    def STDDEV(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate rolling standard deviation"""

        def rolling_std(group):
            group = group.sort_index(level='date')
            result = group.rolling(window=window, min_periods=max(2, window // 4)).std()
            return result

        result = series.groupby(level='symbol', group_keys=False).apply(rolling_std)
        return result

    @staticmethod
    def CORRELATION(series1: pd.Series, series2: pd.Series, window: int = 20) -> pd.Series:
        """Calculate rolling correlation coefficient

        Args:
            series1: First series
            series2: Second series
            window: Rolling window size

        Returns:
            pd.Series: Correlation coefficient series
        """
        # Handle FactorSeries type
        if hasattr(series1, 'series'):
            series1 = series1.series
        if hasattr(series2, 'series'):
            series2 = series2.series

        # Directly calculate rolling correlation
        return series1.rolling(window=window, min_periods=window // 2).corr(series2)

    @staticmethod
    def IF(condition, true_value, false_value):
        """Conditional selection function"""
        return pd.Series(np.where(condition, true_value, false_value), index=condition.index)

    @staticmethod
    def DELAY(series: pd.Series, period: int = 1) -> pd.Series:
        """Calculate lagged values"""
        return series.groupby(level='symbol').shift(period)

    @staticmethod
    def SUM(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate rolling sum"""
        # Handle FactorSeries type
        if hasattr(series, 'series'):
            series = series.series

        return series.groupby(level='symbol').rolling(window=window, min_periods=1).sum().droplevel(0)

    @staticmethod
    def TS_ARGMAX(series: pd.Series, window: int) -> pd.Series:
        """
        Calculate time series maximum value position
        Returns position normalized to [0, 1] range, 0 means earliest, 1 means latest
        """

        def rolling_argmax(group):
            group = group.sort_index()
            result = pd.Series(index=group.index, dtype=float)

            for i in range(len(group)):
                start_idx = max(0, i - window + 1)
                window_data = group.iloc[start_idx:i + 1]

                if window_data.isna().all():
                    result.iloc[i] = np.nan
                    continue

                window_data_valid = window_data.fillna(-np.inf)
                max_val = window_data_valid.max()
                max_positions = np.where(window_data_valid == max_val)[0]

                weights = np.exp(max_positions / len(window_data))
                avg_pos = np.average(max_positions, weights=weights)

                normalized_pos = avg_pos / (len(window_data) - 1) if len(window_data) > 1 else 0
                result.iloc[i] = normalized_pos

            return result

        result = series.groupby(level='symbol', group_keys=False).apply(rolling_argmax)
        if isinstance(result.index, pd.MultiIndex) and len(result.index.names) > 2:
            result = result.droplevel(0)
        return result

    # @staticmethod
    # def SIGNEDPOWER(series: pd.Series, power: float) -> pd.Series:
    #     """Calculate signed power"""
    #     return np.sign(series) * np.abs(series) ** power

    @staticmethod
    def TS_RANK(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate time series rank"""

        def ts_rank(group):
            return group.rolling(window=window, min_periods=1).apply(
                lambda x: pd.Series(x).rank(pct=True).iloc[-1]
            )

        return series.groupby(level='symbol').apply(ts_rank).droplevel(0)

    @staticmethod
    def DELTA(series: pd.Series, period: int = 1) -> pd.Series:
        """Calculate difference"""
        return series.groupby(level='symbol').diff(period)

    @staticmethod
    def ADV(volume: pd.Series, window: int = 20) -> pd.Series:
        """Calculate average daily volume"""
        return volume.groupby(level='symbol').rolling(window=window).mean().droplevel(0)

    @staticmethod
    def TS_MIN(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate time series minimum"""
        return series.groupby(level='symbol').rolling(window=window, min_periods=1).min().droplevel(0)

    @staticmethod
    def TS_MAX(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate time series maximum"""
        return series.groupby(level='symbol').rolling(window=window, min_periods=1).max().droplevel(0)

    @staticmethod
    def TS_ARGMIN(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate time series minimum value position"""

        def rolling_argmin(group):
            return group.rolling(window=window, min_periods=1).apply(
                lambda x: pd.Series(x).argmin()
            )

        return series.groupby(level='symbol').apply(rolling_argmin).droplevel(0)

    @staticmethod
    def DECAY_LINEAR(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate linear decay weighted average"""
        weights = np.linspace(1, 0, window)

        def weighted_mean(x):
            return np.average(x, weights=weights[:len(x)])

        return series.groupby(level='symbol').rolling(window=window, min_periods=1).apply(
            lambda x: weighted_mean(x), raw=True
        ).droplevel(0)

    @staticmethod
    def SCALE(series: pd.Series) -> pd.Series:
        """Scale series to [-1, 1] range"""
        # Save original index
        original_index = series.index

        def scale_group(group):
            min_val = group.min()
            max_val = group.max()
            # Handle case where min == max to avoid division by zero
            if min_val == max_val:
                return pd.Series(0, index=group.index)
            return 2 * (group - min_val) / (max_val - min_val) - 1

        # Apply scaling by date group
        result = series.groupby(level='date', group_keys=False).apply(scale_group)

        # Ensure the result has the exact same index as the input
        if result.index.names != original_index.names:
            result.index = original_index

        return result

    # TODO
    @staticmethod
    def INDUSTRY_NEUTRALIZE(series: pd.Series) -> pd.Series:
        """Industry neutralization"""
        return series.groupby(level='date').apply(lambda x: x - x.mean())

    @staticmethod
    def PRODUCT(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate rolling product"""
        return series.groupby(level='symbol').rolling(window=window, min_periods=1).apply(
            lambda x: np.prod(x)
        ).droplevel(0)

    @staticmethod
    def LOG(series: pd.Series) -> pd.Series:
        """Calculate natural logarithm"""
        return pd.Series(np.log(series), index=series.index)

    @staticmethod
    def POWER(series: pd.Series, power: float) -> pd.Series:
        """Calculate power"""
        return pd.Series(np.power(series, power), index=series.index)

    @staticmethod
    def COVARIANCE(series1: pd.Series, series2: pd.Series, window: int = 20) -> pd.Series:
        """Calculate rolling covariance"""

        def rolling_cov(s1, s2, window):
            return s1.rolling(window=window, min_periods=window // 4).cov(s2)

        result = pd.Series(index=series1.index, dtype=float)
        for symbol in series1.index.get_level_values('symbol').unique():
            s1 = series1[series1.index.get_level_values('symbol') == symbol]
            s2 = series2[series2.index.get_level_values('symbol') == symbol]
            s1, s2 = s1.align(s2)
            result[s1.index] = rolling_cov(s1, s2, window)
        return result

    # @staticmethod
    # def SIGN(series: pd.Series) -> pd.Series:
    #     """Calculate sign function
    #     Returns 1 (positive), -1 (negative), 0 (zero)
    #     """
    #     return np.sign(series)

    @staticmethod
    def MIN(series1: pd.Series, series2: pd.Series | float) -> pd.Series:
        """Calculate element-wise minimum of two series or series and scalar"""
        if isinstance(series2, (int, float)):
            return pd.Series(np.minimum(series1, series2), index=series1.index)
        return pd.Series(np.minimum(series1, series2), index=series1.index)

    @staticmethod
    def MAX(series1: pd.Series, series2: pd.Series | float) -> pd.Series:
        """Calculate element-wise maximum of two series or series and scalar"""
        # Handle FactorSeries type
        if hasattr(series1, 'series'):
            series1 = series1.series
        if not isinstance(series2, (int, float)) and hasattr(series2, 'series'):
            series2 = series2.series

        if isinstance(series2, (int, float)):
            return pd.Series(np.maximum(series1, series2), index=series1.index)
        return pd.Series(np.maximum(series1, series2), index=series1.index)

    @staticmethod
    def AS_FLOAT(condition: pd.Series) -> pd.Series:
        """Convert boolean condition to float
        True -> 1.0, False -> 0.0
        """
        return condition.astype(float)

    @staticmethod
    def ABS(series: pd.Series) -> pd.Series:
        """Calculate absolute value"""
        return pd.Series(np.abs(series), index=series.index)

    @staticmethod
    def VWAP(close: pd.Series, volume: pd.Series) -> pd.Series:
        """Calculate volume weighted average price

        Args:
            close: Price series
            volume: Volume series

        Returns:
            pd.Series: Volume weighted average price series
        """
        # Ensure both series have the same index
        close, volume = close.align(volume)

        # Calculate VWAP for each symbol
        def calculate_vwap(group_close, group_volume):
            # Calculate price * volume
            pv = group_close * group_volume
            # Calculate rolling sum of price * volume and volume
            pv_sum = pv.rolling(window=20, min_periods=1).sum()
            v_sum = group_volume.rolling(window=20, min_periods=1).sum()
            # Calculate VWAP
            return pv_sum / v_sum

        # Group by symbol and calculate VWAP
        result = pd.Series(index=close.index, dtype=float)
        for symbol in close.index.get_level_values('symbol').unique():
            mask = close.index.get_level_values('symbol') == symbol
            symbol_close = close[mask]
            symbol_volume = volume[mask]
            result[mask] = calculate_vwap(symbol_close, symbol_volume)

        return result

    @staticmethod
    def CAP(close: pd.Series, shares: pd.Series) -> pd.Series:
        """Calculate market capitalization
        Args:
            close: Closing price
            shares: Number of outstanding shares
        """
        return close * shares

    # ------------------ Level 0: Core utility functions --------------------------------------------
    @staticmethod
    def RD(S: pd.Series, D=3) -> pd.Series:
        """Round to D decimal places while preserving index"""
        return S.round(D)

    @staticmethod
    def RET(S: pd.Series, N=1) -> pd.Series:
        """Return the Nth last value of series while preserving index"""
        return pd.Series(S.iloc[-N], index=S.index)

    @staticmethod
    def REF(S: pd.Series, N=1) -> pd.Series:
        """Shift entire series by N periods (generates NAN), preserving index"""
        # Handle FactorSeries type
        if hasattr(S, 'series'):
            S = S.series

        return S.shift(N)

    @staticmethod
    def DIFF(S: pd.Series, N=1) -> pd.Series:
        """Calculate difference between current and previous value, generates NAN at start, preserving index"""
        return S.diff(N)

    @staticmethod
    def STD(S: pd.Series, N: int) -> pd.Series:
        """Calculate N-day standard deviation of series"""
        return S.rolling(N).std(ddof=0)

    # @staticmethod
    # def SUM(S: pd.Series, N: int) -> pd.Series:
    #     """Calculate N-day cumulative sum of series, if N=0 calculate running total"""
    #     return S.rolling(N).sum() if N > 0 else S.cumsum()

    @staticmethod
    def CONST(S: pd.Series) -> pd.Series:
        """Return constant series using last value of S"""
        return pd.Series(S.iloc[-1], index=S.index)

    @staticmethod
    def HHV(S: pd.Series, N: int) -> pd.Series:
        """Calculate highest value over N periods"""
        return S.rolling(N).max()

    @staticmethod
    def LLV(S: pd.Series, N: int) -> pd.Series:
        """Calculate lowest value over N periods"""
        return S.rolling(N).min()

    @staticmethod
    def HHVBARS(S: pd.Series, N: int) -> pd.Series:
        """Calculate number of periods since highest value in N periods"""
        return S.rolling(N).apply(lambda x: np.argmax(x[::-1]), raw=True)

    @staticmethod
    def LLVBARS(S: pd.Series, N: int) -> pd.Series:
        """Calculate number of periods since lowest value in N periods"""
        return S.rolling(N).apply(lambda x: np.argmin(x[::-1]), raw=True)

    @staticmethod
    def MA(S: pd.Series, N: int) -> pd.Series:
        """Calculate N-period simple moving average"""
        return S.rolling(N).mean()

    @staticmethod
    def EMA(S: pd.Series, N: int) -> pd.Series:
        """Calculate exponential moving average, requires S>4*N periods for accuracy, EMA needs at least 120 periods, alpha=2/(span+1)"""
        return S.ewm(span=N, adjust=False).mean()

    @staticmethod
    def SMA(S: pd.Series, N: int, M: int = 1) -> pd.Series:
        """Calculate Chinese-style SMA, needs 120 periods for accuracy (180 on XueQiu), alpha=1/(1+com)"""
        return S.ewm(alpha=M / N, adjust=False).mean()

    @staticmethod
    def DMA(S: pd.Series, A: float) -> pd.Series:
        """Calculate dynamic moving average with smoothing factor A, requires 0<A<1"""
        return S.ewm(alpha=A, adjust=False).mean()

    @staticmethod
    def WMA(S: pd.Series, N: int) -> pd.Series:
        """Calculate N-period weighted moving average: Yn = (1*X1+2*X2+3*X3+...+n*Xn)/(1+2+3+...+Xn)"""
        return S.rolling(N).apply(lambda x: x[::-1].cumsum().sum() * 2 / N / (N + 1), raw=True)

    @staticmethod
    def AVEDEV(S: pd.Series, N: int) -> pd.Series:
        """Calculate average absolute deviation (mean absolute difference from mean)"""
        return S.rolling(N).apply(lambda x: (np.abs(x - x.mean())).mean())

    @staticmethod
    def SLOPE(S: pd.Series, N: int) -> pd.Series:
        """Calculate linear regression slope over N periods"""
        return S.rolling(N).apply(lambda x: np.polyfit(range(N), x, deg=1)[0], raw=True)

    @staticmethod
    def FORCAST(S: pd.Series, N: int) -> pd.Series:
        """Calculate predicted value using N-period linear regression"""
        return S.rolling(N).apply(lambda x: np.polyval(np.polyfit(range(N), x, deg=1), N - 1), raw=True)

    @staticmethod
    def LAST(S: pd.Series, A: int, B: int) -> pd.Series:
        """Check if S_BOOL condition holds from A periods ago to B periods ago, requires A>B & A>0 & B>=0"""
        return S.rolling(A + 1).apply(lambda x: np.all(x[::-1][B:]), raw=True).astype(bool)

    @staticmethod
    def DECAYLINEAR(S: pd.Series, d: int) -> pd.Series:
        """Calculate weighted moving average with weights d,d-1,...,1 (normalized to sum to 1)"""
        return S.rolling(d).apply(lambda x: (x * np.arange(1, d + 1)).sum() * 2 / d / (d + 1), raw=True)

    @staticmethod
    def SIGN(S: pd.Series) -> pd.Series:
        """Calculate sign(X) for series"""
        return pd.Series(np.sign(S), index=S.index)

    @staticmethod
    def SIGNEDPOWER(S: pd.Series, n: float) -> pd.Series:
        """Calculate sign(X)*(abs(X)^n)"""
        return pd.Series(np.sign(S) * np.abs(S) ** n, index=S.index)

    # @staticmethod
    # def SCALE(S: pd.Series, a: float = 1) -> pd.Series:
    #     """Return vector a*X/sum(abs(x)), default a=1, a should be positive"""
    #     return a * S / np.sum(np.abs(S))

    # ------------------   Level 1: Application functions (implemented using Level 0 core functions) ----------------------------------
    @staticmethod
    def COUNT(S: pd.Series, N: int) -> pd.Series:
        """COUNT(CLOSE>O, N): Count number of True values in last N days"""
        return FactorUtils.SUM(S, N)

    @staticmethod
    def EVERY(S: pd.Series, N: int) -> pd.Series:
        """EVERY(CLOSE>O, 5) Check if all values are True in last N days"""
        return FactorUtils.IF(FactorUtils.SUM(S, N) == N, True, False)

    @staticmethod
    def EXIST(S: pd.Series, N: int) -> pd.Series:
        """EXIST(CLOSE>3010, N=5) Check if condition exists in last N days"""
        return FactorUtils.IF(FactorUtils.SUM(S, N) > 0, True, False)

    @staticmethod
    def FILTER(S: pd.Series, N: int) -> pd.Series:
        """FILTER function: When S condition is met, set next N periods to 0"""
        result = S.copy()
        for i in range(len(S)):
            if S.iloc[i]:
                result.iloc[i + 1:i + 1 + N] = 0
        return result

    @staticmethod
    def SUMIF(S1: pd.Series, S2: pd.Series, N: int) -> pd.Series:
        """Conditional sum"""
        return pd.Series([s if b else np.nan for s, b in zip(S1, S2)]).rolling(N, min_periods=1).sum()

    @staticmethod
    def BARSLAST(S: pd.Series) -> pd.Series:
        """Calculate periods since last condition was True"""
        M = np.concatenate(([0], np.where(S, 1, 0)))
        for i in range(1, len(M)):
            M[i] = 0 if M[i] else M[i - 1] + 1
        return pd.Series(M[1:], index=S.index)

    @staticmethod
    def BARSLASTCOUNT(S: pd.Series) -> pd.Series:
        """Count consecutive periods where condition S is True"""
        rt = np.zeros(len(S) + 1)
        for i in range(len(S)):
            rt[i + 1] = rt[i] + 1 if S.iloc[i] else rt[i + 1]
        return pd.Series(rt[1:], index=S.index)

    @staticmethod
    def BARSSINCEN(S: pd.Series, N: int) -> pd.Series:
        """Calculate periods since first True condition in last N periods"""
        return S.rolling(N).apply(lambda x: N - 1 - np.argmax(x) if np.argmax(x) or x[0] else 0, raw=True).fillna(
            0).astype(int)

    @staticmethod
    def CROSS(S1: pd.Series, S2: pd.Series) -> pd.Series:
        """Check for golden cross (upward cross) or death cross (downward cross)"""
        # Ensure series are aligned
        S1, S2 = S1.align(S2)
        # Calculate cross condition: previous period S1<=S2, current period S1>S2
        cross_up = (S1 > S2) & (S1.shift(1) <= S2.shift(1))
        return cross_up

    @staticmethod
    def LONGCROSS(S1: pd.Series, S2: pd.Series, N: int) -> pd.Series:
        """Check if series cross after maintaining relative position for N periods"""
        return pd.Series(np.logical_and(FactorUtils.LAST(S1 < S2, N, 1), (S1 > S2)), index=S1.index)

    @staticmethod
    def VALUEWHEN(S: pd.Series, X: pd.Series) -> pd.Series:
        """When condition S is True, take current value of X

        Args:
            S: Condition series (boolean)
            X: Value series

        Returns:
            pd.Series: When condition is True, take current value of X
        """
        # Ensure both series have the same index and are properly aligned
        S, X = S.align(X)

        # Group by symbol and apply the logic
        def apply_valuewhen(group):
            s_group = S.loc[group.index]
            x_group = X.loc[group.index]
            # When condition is True, take the value from X
            return pd.Series(np.where(s_group, x_group, np.nan), index=group.index)

        # Group by symbol and apply the function
        result = X.groupby(level='symbol', group_keys=False).apply(apply_valuewhen)

        # Ensure the result has the correct index names
        if not isinstance(result.index, pd.MultiIndex):
            result.index = pd.MultiIndex.from_tuples(
                [(d, s) for d, s in zip(result.index, result.index)],
                names=['date', 'symbol']
            )
        elif result.index.names != ['date', 'symbol']:
            result.index.names = ['date', 'symbol']

        return result

    # ------------------   Level 2: Technical indicator functions (implemented using Level 0 and 1 functions) ------------------------------
    @staticmethod
    def MACD(CLOSE: pd.Series, SHORT: int = 12, LONG: int = 26, M: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """Calculate MACD indicator using EMA, requires 120 days for accuracy

        Args:
            CLOSE: Close price series
            SHORT: Short period EMA, default 12
            LONG: Long period EMA, default 26
            M: Signal line period, default 9

        Returns:
            Tuple[pd.Series, pd.Series, pd.Series]: DIF (MACD line), DEA (signal line), MACD histogram
        """
        DIF = FactorUtils.EMA(CLOSE, SHORT) - FactorUtils.EMA(CLOSE, LONG)
        DEA = FactorUtils.EMA(DIF, M)
        MACD = (DIF - DEA) * 2
        return DIF, DEA, FactorUtils.RD(MACD)

    @staticmethod
    def KDJ(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, N: int = 9, M1: int = 3, M2: int = 3) -> pd.Series:
        """Calculate KDJ indicator, returns K line

        Args:
            CLOSE: Close price series
            HIGH: High price series
            LOW: Low price series
            N: RSV calculation period, default 9
            M1: K line smoothing period, default 3
            M2: D line smoothing period, default 3

        Returns:
            pd.Series: K line value
        """
        # Calculate RSV in a vectorized way
        llv = LOW.groupby(level='symbol').transform(lambda x: x.rolling(window=N, min_periods=1).min())
        hhv = HIGH.groupby(level='symbol').transform(lambda x: x.rolling(window=N, min_periods=1).max())

        # Vectorized RSV calculation
        rsv = (CLOSE - llv) / (hhv - llv) * 100

        # Vectorized EMA calculation
        alpha = 2 / (M1 + 1)
        K = rsv.groupby(level='symbol').transform(lambda x: x.ewm(alpha=alpha, min_periods=1, adjust=False).mean())

        return K

    @staticmethod
    def RSI(CLOSE: pd.Series, N: int = 24) -> pd.Series:
        """Calculate RSI indicator, matches TDX to 2 decimal places"""
        DIF = CLOSE - FactorUtils.REF(CLOSE, 1)
        return FactorUtils.RD(
            FactorUtils.SMA(FactorUtils.MAX(DIF, 0), N) / FactorUtils.SMA(FactorUtils.ABS(DIF), N) * 100)

    @staticmethod
    def WR(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, N: int = 10, N1: int = 6) -> pd.Series:
        """Calculate Williams %R indicator, returns WR line"""
        WR = (FactorUtils.HHV(HIGH, N) - CLOSE) / (FactorUtils.HHV(HIGH, N) - FactorUtils.LLV(LOW, N)) * 100
        return FactorUtils.RD(WR)

    @staticmethod
    def BIAS(CLOSE: pd.Series, L1: int = 6, L2: int = 12, L3: int = 24) -> pd.Series:
        """Calculate BIAS indicator, returns BIAS1 line"""
        BIAS1 = (CLOSE - FactorUtils.MA(CLOSE, L1)) / FactorUtils.MA(CLOSE, L1) * 100
        return FactorUtils.RD(BIAS1)

    @staticmethod
    def BOLL(CLOSE: pd.Series, N: int = 20, P: int = 2) -> pd.Series:
        """Calculate Bollinger Bands, returns middle line"""
        MID = FactorUtils.MA(CLOSE, N)
        return FactorUtils.RD(MID)

    @staticmethod
    def PSY(CLOSE: pd.Series, N: int = 12, M: int = 6) -> pd.Series:
        """Calculate PSY indicator, returns PSY line"""
        PSY = FactorUtils.COUNT(CLOSE > FactorUtils.REF(CLOSE, 1), N) / N * 100
        return FactorUtils.RD(PSY)

    @staticmethod
    def CCI(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, N: int = 14) -> pd.Series:
        """Calculate CCI indicator"""
        TP = (HIGH + LOW + CLOSE) / 3
        return (TP - FactorUtils.MA(TP, N)) / (0.015 * FactorUtils.AVEDEV(TP, N))

    @staticmethod
    def ATR(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, N: int = 20) -> pd.Series:
        """Calculate Average True Range"""
        TR = FactorUtils.MAX(FactorUtils.MAX((HIGH - LOW), FactorUtils.ABS(FactorUtils.REF(CLOSE, 1) - HIGH)),
                             FactorUtils.ABS(FactorUtils.REF(CLOSE, 1) - LOW))
        return FactorUtils.MA(TR, N)

    @staticmethod
    def BBI(CLOSE: pd.Series, M1: int = 3, M2: int = 6, M3: int = 12, M4: int = 20) -> pd.Series:
        """Calculate BBI (Bull and Bear Index)"""
        return (FactorUtils.MA(CLOSE, M1) + FactorUtils.MA(CLOSE, M2) + FactorUtils.MA(CLOSE, M3) + FactorUtils.MA(
            CLOSE, M4)) / 4

    @staticmethod
    def DMI(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, M1: int = 14, M2: int = 6) -> pd.Series:
        """Calculate DMI indicator, returns ADX line"""
        TR = FactorUtils.SUM(
            FactorUtils.MAX(FactorUtils.MAX(HIGH - LOW, FactorUtils.ABS(HIGH - FactorUtils.REF(CLOSE, 1))),
                            FactorUtils.ABS(LOW - FactorUtils.REF(CLOSE, 1))), M1)
        HD = HIGH - FactorUtils.REF(HIGH, 1)
        LD = FactorUtils.REF(LOW, 1) - LOW
        DMP = FactorUtils.SUM(FactorUtils.IF((HD > 0) & (HD > LD), HD, 0), M1)
        DMM = FactorUtils.SUM(FactorUtils.IF((LD > 0) & (LD > HD), LD, 0), M1)
        PDI = DMP * 100 / TR
        MDI = DMM * 100 / TR
        ADX = FactorUtils.MA(FactorUtils.ABS(MDI - PDI) / (PDI + MDI) * 100, M2)
        return ADX

    @staticmethod
    def TAQ(HIGH: pd.Series, LOW: pd.Series, N: int) -> pd.Series:
        """Calculate Tang Aikun Channel indicator, returns upper line"""
        UP = FactorUtils.HHV(HIGH, N)
        return UP

    @staticmethod
    def KTN(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, N: int = 20, M: int = 10) -> pd.Series:
        """Calculate Keltner Channel, returns middle line"""
        MID = FactorUtils.EMA((HIGH + LOW + CLOSE) / 3, N)
        return MID

    @staticmethod
    def TRIX(CLOSE: pd.Series, M1: int = 12, M2: int = 20) -> pd.Series:
        """Calculate TRIX indicator, returns TRIX line"""
        TR = FactorUtils.EMA(FactorUtils.EMA(FactorUtils.EMA(CLOSE, M1), M1), M1)
        TRIX = (TR - FactorUtils.REF(TR, 1)) / FactorUtils.REF(TR, 1) * 100
        return TRIX

    @staticmethod
    def EMV(HIGH: pd.Series, LOW: pd.Series, VOL: pd.Series, N: int = 14, M: int = 9) -> pd.Series:
        """Calculate EMV indicator, returns EMV line"""
        VOLUME = FactorUtils.MA(VOL, N) / VOL
        MID = 100 * (HIGH + LOW - FactorUtils.REF(HIGH + LOW, 1)) / (HIGH + LOW)
        EMV = FactorUtils.MA(MID * VOLUME * (HIGH - LOW) / FactorUtils.MA(HIGH - LOW, N), N)
        return EMV

    @staticmethod
    def DPO(CLOSE: pd.Series, M1: int = 20, M2: int = 10, M3: int = 6) -> pd.Series:
        """Calculate DPO indicator, returns DPO line"""
        DPO = CLOSE - FactorUtils.REF(FactorUtils.MA(CLOSE, M1), M2)
        return DPO

    @staticmethod
    def BRAR(OPEN: pd.Series, CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, M1: int = 26) -> pd.Series:
        """Calculate BRAR indicator, returns AR line"""
        AR = FactorUtils.SUM(HIGH - OPEN, M1) / FactorUtils.SUM(OPEN - LOW, M1) * 100
        return AR

    @staticmethod
    def DFMA(CLOSE: pd.Series, N1: int = 10, N2: int = 50, M: int = 10) -> pd.Series:
        """Calculate DFMA indicator, returns DIF line"""
        DIF = FactorUtils.MA(CLOSE, N1) - FactorUtils.MA(CLOSE, N2)
        return DIF

    @staticmethod
    def MTM(CLOSE: pd.Series, N: int = 12, M: int = 6) -> pd.Series:
        """Calculate MTM indicator, returns MTM line"""
        MTM = CLOSE - FactorUtils.REF(CLOSE, N)
        return MTM

    @staticmethod
    def MASS(HIGH: pd.Series, LOW: pd.Series, N1: int = 9, N2: int = 25, M: int = 6) -> pd.Series:
        """Calculate MASS indicator, returns MASS line"""
        MASS = FactorUtils.SUM(FactorUtils.MA(HIGH - LOW, N1) / FactorUtils.MA(FactorUtils.MA(HIGH - LOW, N1), N1), N2)
        return MASS

    @staticmethod
    def ROC(CLOSE: pd.Series, N: int = 12) -> pd.Series:
        """Calculate Rate of Change (ROC) indicator

        ROC = (Current Price - Price N periods ago) / Price N periods ago × 100

        Args:
            CLOSE: Price series (typically closing prices)
            N: Number of periods to look back, default 12

        Returns:
            pd.Series: ROC values in percentage
        """
        # Calculate N-period price change rate
        prev_price = CLOSE.groupby(level='symbol').shift(N)
        roc = (CLOSE - prev_price) / prev_price * 100

        # Fill initial NaN values with 0
        roc = roc.fillna(0)

        return roc

    @staticmethod
    def EXPMA(CLOSE: pd.Series, N1: int = 12, N2: int = 50) -> pd.Series:
        """Calculate EXPMA indicator, returns short-term EMA"""
        return FactorUtils.EMA(CLOSE, N1)

    @staticmethod
    def OBV(CLOSE: pd.Series, VOL: pd.Series) -> pd.Series:
        """On Balance Volume

        OBV is calculated by adding volume on up days and subtracting volume on down days.

        Args:
            CLOSE: Close price series
            VOL: Volume series

        Returns:
            pd.Series: OBV values
        """
        # Calculate price changes
        price_changes = CLOSE - FactorUtils.REF(CLOSE, 1)
        # Create volume series with signs based on price changes
        signed_volume = pd.Series(
            np.where(price_changes > 0, VOL,
                     np.where(price_changes < 0, -VOL, 0)),
            index=VOL.index
        )
        # Calculate cumulative sum for each symbol
        return signed_volume.groupby(level='symbol').cumsum() / 10000

    @staticmethod
    def MFI(CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, VOL: pd.Series, N: int = 14) -> pd.Series:
        """Money Flow Index (Volume RSI)

        MFI is a volume-weighted RSI that shows buying and selling pressure.
        Uses vectorized operations for better performance.

        Args:
            CLOSE: Close price series
            HIGH: High price series
            LOW: Low price series
            VOL: Volume series
            N: Calculation period, default 14

        Returns:
            pd.Series: MFI values ranging from 0 to 100
        """
        # Calculate typical price and money flow in one step
        TYP = (HIGH + LOW + CLOSE) / 3
        raw_money_flow = TYP * VOL

        # Calculate price changes using groupby shift
        price_changes = TYP.groupby(level='symbol').diff()

        # Vectorized calculation of positive and negative money flows
        pos_flow = raw_money_flow.where(price_changes > 0, 0)
        neg_flow = raw_money_flow.where(price_changes < 0, 0)

        # Calculate rolling sums for positive and negative flows
        pos_sum = pos_flow.groupby(level='symbol').rolling(window=N, min_periods=1).sum().droplevel(0)
        neg_sum = neg_flow.groupby(level='symbol').rolling(window=N, min_periods=1).sum().droplevel(0)

        # Calculate money ratio and MFI in one step
        money_ratio = pos_sum / neg_sum.replace(0, 1e-10)  # Avoid division by zero
        mfi = 100 - (100 / (1 + money_ratio))

        # Handle edge cases vectorized
        total_flow = pos_sum + neg_sum
        mfi = np.where(total_flow == 0, 50,  # No money flow
                       np.where(neg_sum == 0, 100,  # No negative flow
                                np.where(pos_sum == 0, 0,  # No positive flow
                                         mfi)))

        return pd.Series(mfi, index=CLOSE.index)

    @staticmethod
    def ASI(OPEN: pd.Series, CLOSE: pd.Series, HIGH: pd.Series, LOW: pd.Series, M1: int = 26,
            M2: int = 10) -> pd.Series:
        """Calculate ASI indicator, returns ASI line"""
        LC = FactorUtils.REF(CLOSE, 1)
        AA = FactorUtils.ABS(HIGH - LC)
        BB = FactorUtils.ABS(LOW - LC)
        CC = FactorUtils.ABS(HIGH - FactorUtils.REF(LOW, 1))
        DD = FactorUtils.ABS(LC - FactorUtils.REF(OPEN, 1))
        R = FactorUtils.IF((AA > BB) & (AA > CC), AA + BB / 2 + DD / 4,
                           FactorUtils.IF((BB > CC) & (BB > AA), BB + AA / 2 + DD / 4, CC + DD / 4))
        X = (CLOSE - LC + (CLOSE - OPEN) / 2 + LC - FactorUtils.REF(OPEN, 1))
        SI = 16 * X / R * FactorUtils.MAX(AA, BB)
        ASI = FactorUtils.SUM(SI, M1)
        return ASI

    @staticmethod
    def TS_MEAN(series: pd.Series, window: int = 20) -> pd.Series:
        """Calculate time series moving average

        Args:
            series: Input series
            window: Moving window size, default 20

        Returns:
            pd.Series: Moving average series
        """
        return series.groupby(level='symbol').rolling(window=window, min_periods=1).mean().droplevel(0)

    # Add other public methods...