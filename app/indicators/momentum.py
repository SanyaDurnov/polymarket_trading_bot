"""
Индикатор Momentum - процентное изменение цены за период.
"""

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def calculate_momentum(
    prices: pd.Series,
    period_seconds: int = 10,
    timestamp_col: Optional[str] = None,
) -> Optional[float]:
    """
    Рассчитать Momentum (процентное изменение цены за период).
    
    Args:
        prices: Series с ценами (или DataFrame с колонкой price)
        period_seconds: Период в секундах для расчета
        timestamp_col: Название колонки с timestamp (если prices - DataFrame)
    
    Returns:
        Momentum в процентах или None, если недостаточно данных
    """
    try:
        # Если передан DataFrame, извлекаем цены
        if isinstance(prices, pd.DataFrame):
            if timestamp_col:
                prices = prices.set_index(timestamp_col)
            if "close" in prices.columns:
                price_series = prices["close"]
            elif "price" in prices.columns:
                price_series = prices["price"]
            else:
                price_series = prices.iloc[:, 0]
        else:
            price_series = prices
        
        if len(price_series) < 2:
            return None
        
        # Берем последние значения
        current_price = price_series.iloc[-1]
        
        # Находим цену N секунд назад
        if isinstance(price_series.index, pd.DatetimeIndex):
            # Если есть временной индекс, используем его
            cutoff_time = price_series.index[-1] - pd.Timedelta(seconds=period_seconds)
            past_prices = price_series[price_series.index >= cutoff_time]
            if len(past_prices) < 2:
                # Если недостаточно данных, берем первое значение
                past_price = price_series.iloc[0]
            else:
                past_price = past_prices.iloc[0]
        else:
            # Если нет временного индекса, берем первое значение
            past_price = price_series.iloc[0]
        
        if past_price == 0:
            return None
        
        # Рассчитываем процентное изменение
        momentum = ((current_price - past_price) / past_price) * 100
        
        return momentum
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка при расчете Momentum: %s", exc)
        return None


class MultiTimeframeAnalyzer:
    """Анализирует momentum на разных таймфреймах."""

    @staticmethod
    def momentum_from_dataframe(
        df: pd.DataFrame,
        minutes: int | None = None,
        seconds: int | None = None,
    ) -> float | None:
        """
        Считает momentum за N минут или N секунд назад.

        Args:
            df: DataFrame с 'close' (индекс - datetime)
            minutes: сколько минут назад (если seconds is None)
            seconds: сколько секунд назад (если minutes is None)

        Returns:
            процентное изменение или None
        """
        if df is None or df.empty or len(df) < 2:
            return None

        now_price = df["close"].iloc[-1]
        now_ts = df.index[-1]

        if minutes is not None:
            cutoff_ts = now_ts - pd.Timedelta(minutes=minutes)
        elif seconds is not None:
            cutoff_ts = now_ts - pd.Timedelta(seconds=seconds)
        else:
            return None

        past_df = df[df.index <= cutoff_ts]
        if past_df.empty:
            return None

        past_price = past_df["close"].iloc[-1]

        if past_price == 0:
            return None

        momentum = ((now_price - past_price) / past_price) * 100.0
        return float(momentum)

    @staticmethod
    def calculate_all_momentums(
        df_1m: pd.DataFrame,
        price_buffer,
        timeframes: list[tuple[int, str]],
        symbol: str = "BTCUSDT",
    ) -> dict[str, float | None]:
        """
        Считает momentum для всех timeframes из конфига.

        Args:
            df_1m: DataFrame с 1m свечами
            price_buffer: PriceBuffer (для tick-данных за последние секунды)
            timeframes: список (value, type) из конфига
            symbol: для логирования

        Returns:
            словарь вида {'15m': 0.45, '5m': 0.23, '1m': 0.10, '10s': 0.08}
        """
        result: dict[str, float | None] = {}

        for value, tf_type in timeframes:
            key = f"{value}{tf_type[0]}"

            if tf_type == "minute":
                momentum = MultiTimeframeAnalyzer.momentum_from_dataframe(
                    df_1m,
                    minutes=value,
                )
            elif tf_type == "second":
                momentum = MultiTimeframeAnalyzer._momentum_from_ticks(
                    price_buffer,
                    seconds=value,
                )
            else:
                logger.warning("Неизвестный timeframe type: %s (symbol=%s)", tf_type, symbol)
                momentum = None

            result[key] = momentum

        return result

    @staticmethod
    def _momentum_from_ticks(price_buffer, seconds: int = 10) -> float | None:
        """
        Считает momentum из буфера тиков (tick-данные).
        """
        from datetime import datetime

        now = datetime.now()
        price_list = list(getattr(price_buffer, "price_buffer", []))

        if not price_list:
            return None

        prices_in_window = [
            (ts, price)
            for ts, price in price_list
            if (now - ts).total_seconds() <= seconds
        ]

        if len(prices_in_window) < 2:
            return None

        price_start = prices_in_window[0][1]
        price_end = prices_in_window[-1][1]

        if price_start == 0:
            return None

        momentum = ((price_end - price_start) / price_start) * 100.0
        return float(momentum)

    @staticmethod
    def resample_to_5min(df_1m: pd.DataFrame) -> pd.DataFrame:
        """Ресамплирует 1m DataFrame в 5m."""
        if df_1m is None or df_1m.empty:
            return pd.DataFrame()

        df = df_1m.copy()
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")

        resampled = df.resample("5min").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna()

    @staticmethod
    def resample_to_15min(df_1m: pd.DataFrame) -> pd.DataFrame:
        """Ресамплирует 1m DataFrame в 15m."""
        if df_1m is None or df_1m.empty:
            return pd.DataFrame()

        df = df_1m.copy()
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")

        resampled = df.resample("15min").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna()

    @staticmethod
    def resample_to_hourly(df_1m: pd.DataFrame) -> pd.DataFrame:
        """Ресамплирует 1m DataFrame в 1h."""
        if df_1m is None or df_1m.empty:
            return pd.DataFrame()

        df = df_1m.copy()
        if "timestamp" in df.columns:
            df = df.set_index("timestamp")

        resampled = df.resample("1h").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        return resampled.dropna()
