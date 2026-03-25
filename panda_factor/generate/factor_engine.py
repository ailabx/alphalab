from panda_factor.generate.factor_utils import FactorUtils

import numpy as np
import pandas as pd
from typing import Dict, List, Union, Optional, Any
import logging
from functools import lru_cache
import inspect

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FactorEngine:
    """
    优化的因子表达式引擎
    """

    # 预定义的数学函数映射
    _MATH_FUNCTIONS = {
        'LOG': np.log, 'EXP': np.exp, 'SQRT': np.sqrt, 'ABS': np.abs,
        'SIN': np.sin, 'COS': np.cos, 'TAN': np.tan, 'POWER': np.power,
        'SIGN': np.sign, 'MAX': np.maximum, 'MIN': np.minimum,
        'MEAN': np.mean, 'STD': np.std, 'SUM': np.sum, 'MEDIAN': np.median,
        'VAR': np.var, 'CEIL': np.ceil, 'FLOOR': np.floor
    }

    # 基础价格字段
    _BASE_COLUMNS = ['open', 'high', 'low', 'close', 'volume']

    def __init__(self, safe_mode: bool = True):
        """
        初始化因子表达式引擎

        Args:
            safe_mode: 安全模式，限制可用的函数和操作
        """
        self.safe_mode = safe_mode
        self._context = self._build_context()
        self._expression_cache = {}

    def _build_context(self) -> Dict[str, Any]:
        """构建安全的执行上下文"""
        context = {}

        # 添加因子扩展函数
        context.update(self._import_functions(FactorUtils))

        # 添加数学函数
        context.update(self._MATH_FUNCTIONS)

        # 安全模式下限制危险函数
        if self.safe_mode:
            self._remove_unsafe_functions(context)

        # 添加必要的模块
        context.update({
            'np': np,
            'pd': pd,
            'len': len,
            'range': range
        })

        logger.info(f"上下文构建完成，包含 {len(context)} 个函数和变量")
        return context

    def _import_functions(self, module) -> Dict[str, Any]:
        """从模块导入函数"""
        functions = {}
        for name in dir(module):
            if not name.startswith('_'):
                try:
                    obj = getattr(module, name)
                    if callable(obj):
                        functions[name] = obj
                        functions[name.upper()] = obj
                except Exception as e:
                    logger.warning(f"导入函数 {name} 失败: {e}")
        return functions

    def _remove_unsafe_functions(self, context: Dict[str, Any]):
        """移除不安全的函数"""
        unsafe_patterns = ['eval', 'exec', 'compile', 'open', 'file', 'import']
        unsafe_keys = []
        for key in context.keys():
            if any(pattern in key.lower() for pattern in unsafe_patterns):
                unsafe_keys.append(key)

        for key in unsafe_keys:
            del context[key]
        logger.info(f"安全模式已移除 {len(unsafe_keys)} 个不安全函数")

    def _create_data_context(self, df: pd.DataFrame) -> Dict[str, Any]:
        """为特定数据创建上下文"""
        data_context = self._context.copy()

        # 添加基础价格数据
        for col in self._BASE_COLUMNS:
            if col in df.columns:
                data_context[col.upper()] = df[col]
            else:
                logger.warning(f"数据中缺少列: {col}")

        # 添加DataFrame引用用于高级操作
        data_context['df'] = df

        return data_context

    def _validate_dataframe(self, df: pd.DataFrame):
        """验证输入DataFrame的格式"""
        if not isinstance(df, pd.DataFrame):
            raise ValueError("输入必须是pandas DataFrame")

        if df.empty:
            raise ValueError("输入DataFrame为空")

        # 检查必要的列
        missing_cols = [col for col in self._BASE_COLUMNS if col not in df.columns]
        if missing_cols:
            logger.warning(f"缺少基础价格列: {missing_cols}")

    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """预处理DataFrame"""
        df = df.copy()

        # 确保索引是日期时间类型
        if 'date' in df.columns:
            df = df.set_index('date')

        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception as e:
                logger.warning(f"无法将索引转换为DatetimeIndex: {e}")

        # 排序并去重
        df = df.sort_index()
        df = df[~df.index.duplicated(keep='first')]

        return df

    def _postprocess_result(self, result: Any, expr: str, original_index: pd.Index) -> Union[
        pd.Series, List[pd.Series]]:
        """后处理计算结果"""
        if result is None:
            raise ValueError(f"表达式 '{expr}' 返回了 None")

        # 处理元组或列表结果
        if isinstance(result, (tuple, list)):
            results = []
            for i, r in enumerate(result):
                if r is not None:
                    series = self._create_result_series(r, f"{expr}_{i}", original_index)
                    results.append(series)
            return results if len(results) > 1 else results[0] if results else None

        # 处理单个结果
        return self._create_result_series(result, expr, original_index)

    def _create_result_series(self, data: Any, name: str, original_index: pd.Index) -> pd.Series:
        """创建结果Series"""
        if isinstance(data, pd.Series):
            series = data
        else:
            # 确保数据是numpy数组
            data_array = np.asarray(data)
            if len(data_array) != len(original_index):
                raise ValueError(f"结果长度 {len(data_array)} 与索引长度 {len(original_index)} 不匹配")
            series = pd.Series(data_array, index=original_index, name=name)

        series.name = name
        return series

    @lru_cache(maxsize=100)
    def _get_cached_expression(self, expr: str) -> str:
        """缓存表达式处理结果"""
        return expr.upper().strip()

    def calc_formula(self, df: pd.DataFrame, expr: str) -> Union[pd.Series, List[pd.Series]]:
        """
        计算单个因子表达式

        Args:
            df: 输入数据
            expr: 因子表达式

        Returns:
            计算结果Series或Series列表
        """
        try:
            # 输入验证
            self._validate_dataframe(df)
            if not expr or not isinstance(expr, str):
                raise ValueError("表达式必须是非空字符串")

            # 预处理数据
            processed_df = self._preprocess_dataframe(df)
            original_index = processed_df.index

            # 创建数据上下文
            context = self._create_data_context(processed_df)

            # 处理表达式
            processed_expr = self._get_cached_expression(expr)
            logger.debug(f"计算表达式: {processed_expr}")

            # 执行计算
            result = eval(processed_expr, {"__builtins__": {}}, context)

            # 后处理结果
            return self._postprocess_result(result, expr, original_index)

        except Exception as e:
            error_msg = f"计算表达式 '{expr}' 时出错: {str(e)}"
            logger.error(error_msg)
            # 提供更详细的错误信息
            self._log_debug_info(expr, df.columns.tolist() if df is not None else [])
            raise ValueError(error_msg) from e

    def _log_debug_info(self, expr: str, available_columns: List[str]):
        """记录调试信息"""
        logger.debug(f"表达式: {expr}")
        logger.debug(f"可用数据列: {available_columns}")
        logger.debug(f"可用函数: {list(self._context.keys())[:20]}...")  # 只显示前20个

    def calc_formulas(self, dfs: Dict[str, pd.DataFrame], expr_list: List[str],
                      skip_existing: bool = True) -> pd.DataFrame:
        """
        批量计算多个因子表达式

        Args:
            dfs: 股票数据字典 {symbol: dataframe}
            expr_list: 表达式列表
            skip_existing: 是否跳过已存在的列

        Returns:
            合并后的结果DataFrame
        """
        if not dfs:
            raise ValueError("输入数据字典不能为空")

        if not expr_list:
           return None

        all_results = {}

        for symbol, df in dfs.items():
            try:
                logger.info(f"处理股票 {symbol}")

                # 预处理数据
                processed_df = self._preprocess_dataframe(df)
                #print(processed_df)
                results = []

                for expr in expr_list:
                    # 检查是否跳过已存在的列
                    if skip_existing and expr in processed_df.columns:
                        logger.debug(f"股票 {symbol} 已存在列 '{expr}'，跳过计算")
                        continue

                    # 计算因子
                    result = self.calc_formula(processed_df, expr)

                    # 处理多个返回值
                    if isinstance(result, list):
                        results.extend(result)
                    else:
                        results.append(result)

                # 合并结果
                if results:
                    # 添加原始数据
                    results.append(processed_df)
                    symbol_results = pd.concat(results, axis=1)
                    symbol_results['symbol'] = symbol
                    all_results[symbol] = symbol_results

            except Exception as e:
                logger.error(f"处理股票 {symbol} 时出错: {e}")
                continue

        if not all_results:
            raise ValueError("所有股票处理失败，无有效结果")
        return all_results

        # # 合并所有股票数据
        # final_result = pd.concat(all_results, axis=0)
        # final_result = final_result.sort_index()
        #
        # # 最终去重
        # final_result = final_result[~final_result.index.duplicated(keep='first')]
        #
        # logger.info(f"计算完成，最终数据形状: {final_result.shape}")
        #return final_result

    def get_available_functions(self) -> List[str]:
        """获取所有可用的函数列表"""
        return [name for name, obj in self._context.items() if callable(obj)]

    def validate_expression(self, expr: str) -> bool:
        """验证表达式语法"""
        try:
            processed_expr = self._get_cached_expression(expr)
            compile(processed_expr, '<string>', 'eval')
            return True
        except Exception as e:
            logger.warning(f"表达式验证失败: {e}")
            return False


# 使用示例
if __name__ == '__main__':
    from datafeed.sqlite_dataloader import load_data
    df = load_data()

    # 创建引擎实例
    factor_engine = FactorEngine(safe_mode=True)

    ret = factor_engine.calc_formula(df, 'CORRELATION(CLOSE, VOLUME, 20)')
    print(ret)