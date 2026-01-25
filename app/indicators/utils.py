"""
Утилиты для расчета индикаторов технического анализа.
"""

import logging
from typing import Dict, Optional

import pandas as pd
import ta

logger = logging.getLogger(__name__)


def calculate_rsi(prices: pd.Series, period: int = 14) -> Optional[float]:
    """
    Рассчитать RSI (Relative Strength Index).
    
    Args:
        prices: Series с ценами
        period: Период RSI (по умолчанию 14)
    
    Returns:
        Значение RSI или None, если недостаточно данных
    """
    try:
        if len(prices) < period + 1:
            return None
        
        rsi = ta.momentum.RSIIndicator(close=prices, window=period)
        rsi_values = rsi.rsi()
        
        if rsi_values.empty or pd.isna(rsi_values.iloc[-1]):
            return None
        
        return float(rsi_values.iloc[-1])
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка при расчете RSI: %s", exc)
        return None


def calculate_macd(
    prices: pd.Series,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> Optional[Dict[str, float]]:
    """
    Рассчитать MACD (Moving Average Convergence Divergence).
    
    Args:
        prices: Series с ценами
        fast: Период быстрой EMA
        slow: Период медленной EMA
        signal: Период сигнальной линии
    
    Returns:
        Словарь с macd, signal, histogram или None
    """
    try:
        if len(prices) < slow + signal:
            return None
        
        macd_indicator = ta.trend.MACD(
            close=prices,
            window_fast=fast,
            window_slow=slow,
            window_sign=signal,
        )
        
        macd_line = macd_indicator.macd()
        signal_line = macd_indicator.macd_signal()
        histogram = macd_indicator.macd_diff()
        
        if (
            macd_line.empty
            or signal_line.empty
            or histogram.empty
            or pd.isna(macd_line.iloc[-1])
            or pd.isna(signal_line.iloc[-1])
        ):
            return None
        
        return {
            "macd": float(macd_line.iloc[-1]),
            "signal": float(signal_line.iloc[-1]),
            "histogram": float(histogram.iloc[-1]),
        }
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка при расчете MACD: %s", exc)
        return None


def calculate_atr(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 14,
) -> Optional[float]:
    """
    Рассчитать ATR (Average True Range).
    
    Args:
        high: Series с максимальными ценами
        low: Series с минимальными ценами
        close: Series с ценами закрытия
        period: Период ATR (по умолчанию 14)
    
    Returns:
        Значение ATR или None, если недостаточно данных
    """
    try:
        if len(high) < period + 1 or len(low) < period + 1 or len(close) < period + 1:
            return None
        
        atr_indicator = ta.volatility.AverageTrueRange(
            high=high,
            low=low,
            close=close,
            window=period,
        )
        atr_values = atr_indicator.average_true_range()
        
        if atr_values.empty or pd.isna(atr_values.iloc[-1]):
            return None
        
        return float(atr_values.iloc[-1])
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка при расчете ATR: %s", exc)
        return None


def calculate_sma(prices: pd.Series, period: int = 5) -> Optional[float]:
    """
    Рассчитать SMA (Simple Moving Average).
    
    Args:
        prices: Series с ценами
        period: Период SMA (по умолчанию 5)
    
    Returns:
        Значение SMA или None, если недостаточно данных
    """
    try:
        if len(prices) < period:
            return None
        
        sma = prices.rolling(window=period).mean()
        
        if sma.empty or pd.isna(sma.iloc[-1]):
            return None
        
        return float(sma.iloc[-1])
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка при расчете SMA: %s", exc)
        return None
