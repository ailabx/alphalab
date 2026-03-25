# FactorUtils 函数列表

## 基础计算函数
- RANK(series) - 横截面排名，归一化到[-0.5, 0.5]范围
- RETURNS(close, period=1) - 计算收益率
- STDDEV(series, window=20) - 计算滚动标准差
- CORRELATION(series1, series2, window=20) - 计算滚动相关系数
- IF(condition, true_value, false_value) - 条件选择函数
- MIN(series1, series2) - 取两个序列对应位置的最小值
- MAX(series1, series2) - 取两个序列对应位置的最大值
- ABS(series) - 计算绝对值
- LOG(series) - 计算自然对数
- POWER(series, power) - 计算幂次方
- SIGN(series) - 计算符号值（1, 0, -1）
- SIGNEDPOWER(series, power) - 计算带符号的幂次方
- COVARIANCE(series1, series2, window=20) - 计算滚动协方差

## 时间序列函数
- DELAY(series, period=1) - 序列延迟，返回N周期前的值
- SUM(series, window=20) - 计算移动求和
- TS_ARGMAX(series, window) - 返回窗口内最大值的位置
- TS_ARGMIN(series, window) - 返回窗口内最小值的位置
- TS_MEAN(series, window=20) - 计算移动平均
- TS_MIN(series, window=20) - 计算移动最小值
- TS_MAX(series, window=20) - 计算移动最大值
- TS_RANK(series, window=20) - 计算时间序列排名
- DECAY_LINEAR(series, window=20) - 线性衰减加权
- MA(series, window) - 简单移动平均
- EMA(series, window) - 指数移动平均
- SMA(series, window, M=1) - 平滑移动平均
- DMA(series, A) - 动态移动平均
- WMA(series, window) - 加权移动平均

## 技术指标函数
- MACD(close, SHORT=12, LONG=26, M=9) - 计算MACD指标，返回快线、慢线和MACD值
- KDJ(close, high, low, N=9, M1=3, M2=3) - 计算KDJ指标，返回K、D、J值
- RSI(close, N=24) - 计算相对强弱指标
- BOLL(close, N=20, P=2) - 计算布林带，返回上中下轨
- CCI(close, high, low, N=14) - 计算顺势指标
- ATR(close, high, low, N=20) - 计算真实波动幅度均值
- DMI(close, high, low, M1=14, M2=6) - 计算动向指标，返回PDI、MDI、ADX、ADXR
- BBI(close, M1=3, M2=6, M3=12, M4=20) - 计算多空指标
- TAQ(high, low, N) - 计算唐安奇通道指标
- KTN(close, high, low, N=20, M=10) - 计算肯特纳通道
- TRIX(close, M1=12, M2=20) - 计算三重指数平滑平均线
- VR(close, vol, M1=26) - 计算成交量比率
- EMV(high, low, vol, N=14, M=9) - 计算简易波动指标
- DPO(close, M1=20, M2=10, M3=6) - 计算区间震荡线
- BRAR(open, close, high, low, M1=26) - 计算情绪指标
- MTM(close, N=12, M=6) - 计算动量指标
- MASS(high, low, N1=9, N2=25, M=6) - 计算梅斯线
- ROC(close, N=12, M=6) - 计算变动率
- EXPMA(close, N1=12, N2=50) - 计算EMA指数平均数
- OBV(close, vol) - 计算能量潮指标
- MFI(close, high, low, vol, N=14) - 计算资金流量指标
- ASI(open, close, high, low, M1=26, M2=10) - 计算振动升降指标
- PSY(close, N=12, M=6) - 计算心理线指标
- BIAS(close, L1=6, L2=12, L3=24) - 计算乖离率
- WR(close, high, low, N=10, N1=6) - 计算威廉指标

## 价格类函数
- VWAP(close, volume) - 计算成交量加权平均价格
- CAP(close, shares) - 计算市值

## 核心工具函数
- RD(S, D=3) - 四舍五入取D位小数
- RET(S, N=1) - 返回序列倒数第N个值
- REF(S, N=1) - 对序列整体下移动N
- DIFF(S, N=1) - 计算前后差值
- CONST(S) - 返回序列最后的值组成常量序列
- HHVBARS(S, N) - 求N周期内最高值到当前的周期数
- LLVBARS(S, N) - 求N周期内最低值到当前的周期数
- AVEDEV(S, N) - 计算平均绝对偏差
- SLOPE(S, N) - 计算线性回归斜率
- FORCAST(S, N) - 计算线性回归预测值
- LAST(S, A, B) - 判断从前A日到前B日是否一直满足条件
- COUNT(S, N) - 统计N周期内满足条件的周期数
- EVERY(S, N) - 判断N周期内是否都满足条件
- EXIST(S, N) - 判断N周期内是否存在满足条件的周期
- FILTER(S, N) - 信号过滤，N周期内仅保留第一个信号
- SUMIF(S1, S2, N) - 对满足条件的数据求N周期和
- BARSLAST(S) - 上一次条件成立到当前的周期数
- BARSLASTCOUNT(S) - 统计连续满足条件的周期数
- BARSSINCEN(S, N) - 统计N周期内满足条件的周期数
- CROSS(S1, S2) - 判断向上金叉穿越
- LONGCROSS(S1, S2, N) - 判断是否维持N个周期的金叉
- VALUEWHEN(S, X) - 当条件成立时取值

## 注意事项
1. 所有函数都支持多股票并行计算
2. 输入序列需要包含正确的索引（date和symbol两级）
3. 返回的序列会保持原有的索引结构
4. 部分函数会自动处理缺失值（NaN）
5. 时间序列函数都会考虑股票分组进行计算 