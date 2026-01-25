"""
Price Buffer для хранения цен в памяти.

Использует collections.deque для эффективного хранения последних N точек цен и свечей.
"""

import logging
from collections import deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class PriceBuffer:
    """
    Буфер для хранения цен и свечей в памяти.
    
    Хранит:
    - Последние N точек цен (price_buffer_size)
    - Свечи 1m (kline_buffer_size_1m)
    - Свечи 5m (kline_buffer_size_5m)
    """
    
    def __init__(
        self,
        price_buffer_size: int = 3600,
        kline_buffer_size_1m: int = 1440,
        kline_buffer_size_5m: int = 288,
    ):
        """
        Инициализация буфера.
        
        Args:
            price_buffer_size: Максимальное количество точек цен (по умолчанию 3600 = 1 час)
            kline_buffer_size_1m: Максимальное количество 1m свеч (по умолчанию 1440 = 24 часа)
            kline_buffer_size_5m: Максимальное количество 5m свеч (по умолчанию 288 = 24 часа)
        """
        self.price_buffer_size = price_buffer_size
        self.kline_buffer_size_1m = kline_buffer_size_1m
        self.kline_buffer_size_5m = kline_buffer_size_5m
        
        # Буферы для цен (timestamp, price)
        self.price_buffer: deque = deque(maxlen=price_buffer_size)
        
        # Буферы для свечей (по интервалу)
        self.kline_buffers: Dict[str, deque] = {
            "1m": deque(maxlen=kline_buffer_size_1m),
            "5m": deque(maxlen=kline_buffer_size_5m),
        }
        
        logger.info(
            "PriceBuffer инициализирован: price_buffer_size=%s, 1m=%s, 5m=%s",
            price_buffer_size,
            kline_buffer_size_1m,
            kline_buffer_size_5m,
        )
    
    def add_price(self, price: float, timestamp: datetime) -> None:
        """
        Добавить точку цены в буфер.
        
        Args:
            price: Цена
            timestamp: Временная метка
        """
        self.price_buffer.append((timestamp, price))
    
    def add_kline(self, kline_dict: Dict, interval: str = "1m") -> None:
        """
        Добавить свечу в буфер.
        
        Args:
            kline_dict: Словарь со свечой (должен содержать open, high, low, close, volume, timestamp)
            interval: Интервал свечи ("1m" или "5m")
        """
        if interval not in self.kline_buffers:
            logger.warning("Неизвестный интервал свечи: %s", interval)
            return
        
        # Нормализуем формат свечи
        if isinstance(kline_dict, dict):
            kline = {
                "timestamp": kline_dict.get("timestamp") or kline_dict.get("time"),
                "open": float(kline_dict.get("open", 0)),
                "high": float(kline_dict.get("high", 0)),
                "low": float(kline_dict.get("low", 0)),
                "close": float(kline_dict.get("close", 0)),
                "volume": float(kline_dict.get("volume", 0)),
            }
            self.kline_buffers[interval].append(kline)
        else:
            logger.warning("Неверный формат свечи: %s", type(kline_dict))
    
    def get_price_history(self, last_n_seconds: int) -> List[Tuple[datetime, float]]:
        """
        Получить историю цен за последние N секунд.
        
        Args:
            last_n_seconds: Количество секунд назад
        
        Returns:
            Список кортежей (timestamp, price)
        """
        if not self.price_buffer:
            return []
        
        cutoff_time = datetime.now() - timedelta(seconds=last_n_seconds)
        return [
            (ts, price)
            for ts, price in self.price_buffer
            if ts >= cutoff_time
        ]
    
    def to_dataframe(self, kline_type: str = "1m") -> Optional[pd.DataFrame]:
        """
        Конвертировать свечи в pandas DataFrame.
        
        Args:
            kline_type: Тип свечей ("1m" или "5m")
        
        Returns:
            DataFrame со свечами или None, если буфер пуст
        """
        if kline_type not in self.kline_buffers:
            logger.warning("Неизвестный тип свечей: %s", kline_type)
            return None
        
        buffer = self.kline_buffers[kline_type]
        if not buffer:
            return None
        
        # Конвертируем deque в список словарей
        klines = list(buffer)
        
        # Создаем DataFrame
        df = pd.DataFrame(klines)
        
        # Конвертируем timestamp в datetime, если это не так
        if "timestamp" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
                # Пробуем разные форматы timestamp
                if isinstance(df["timestamp"].iloc[0], (int, float)):
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                else:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Устанавливаем timestamp как индекс
            df.set_index("timestamp", inplace=True)
        
        return df
    
    def get_latest_price(self) -> Optional[float]:
        """Получить последнюю цену из буфера."""
        if not self.price_buffer:
            return None
        return self.price_buffer[-1][1]
    
    def get_latest_kline(self, interval: str = "1m") -> Optional[Dict]:
        """Получить последнюю свечу из буфера."""
        if interval not in self.kline_buffers:
            return None
        if not self.kline_buffers[interval]:
            return None
        return self.kline_buffers[interval][-1]
    
    def clear(self) -> None:
        """Очистить все буферы."""
        self.price_buffer.clear()
        for buffer in self.kline_buffers.values():
            buffer.clear()
        logger.info("PriceBuffer очищен")
