"""
Binance Real-Time Price Connector с WebSocket.

Использует python-binance для подписки на цены BTCUSDT и ETHUSDT в реальном времени.
"""

import asyncio
import logging
from datetime import datetime
from typing import Callable, Optional

from binance import AsyncClient, BinanceSocketManager
from binance.exceptions import BinanceAPIException

logger = logging.getLogger(__name__)


class BinanceConnector:
    """
    Connector для получения цен Binance через WebSocket.
    
    Подписывается на:
    - BTCUSDT
    - ETHUSDT
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        symbols: Optional[list[str]] = None,
    ):
        """
        Инициализация Binance connector.
        
        Args:
            api_key: API ключ Binance (опционально, для публичных данных не нужен)
            api_secret: API секрет Binance (опционально)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.client: Optional[AsyncClient] = None
        self.socket_manager: Optional[BinanceSocketManager] = None
        self.sockets: list = []
        self.socket_tasks: list[asyncio.Task] = []
        self.is_running = False
        self.symbols = symbols or ["BTCUSDT", "ETHUSDT"]
        
        # Callback для обработки обновлений цен
        self.price_callback: Optional[Callable[[str, float, datetime], None]] = None
        
        # Последние цены
        self.last_prices: dict[str, float] = {}
        
        logger.info("BinanceConnector инициализирован")

    async def _ensure_client(self) -> None:
        """Создать AsyncClient, если он еще не инициализирован."""
        if self.client:
            return

        self.client = await AsyncClient.create(
            api_key=self.api_key or "",
            api_secret=self.api_secret or "",
        )
    
    async def start(self) -> None:
        """Запустить WebSocket соединения."""
        try:
            # Создаем асинхронный клиент
            await self._ensure_client()
            if not self.socket_manager:
                self.socket_manager = BinanceSocketManager(self.client)
            
            # Подписываемся на цены
            symbols = self.symbols
            self.is_running = True
            
            for symbol in symbols:
                # Используем stream последней цены для символа
                socket = self.socket_manager.symbol_ticker_socket(symbol)
                self.sockets.append((symbol, socket))
                
                async def read_socket(sym: str, sock) -> None:
                    try:
                        async with sock as stream:
                            while self.is_running:
                                msg = await stream.recv()
                                if not isinstance(msg, dict):
                                    continue
                                if "c" in msg:  # 'c' - последняя цена
                                    price = float(msg["c"])
                                    timestamp = datetime.now()
                                    
                                    self.last_prices[sym] = price
                                    
                                    # Вызываем callback, если установлен
                                    if self.price_callback:
                                        self.price_callback(sym, price, timestamp)
                                    
                                    logger.debug(
                                        "%s: $%.2f @ %s",
                                        sym,
                                        price,
                                        timestamp.strftime("%H:%M:%S"),
                                    )
                    except asyncio.CancelledError:
                        return
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Ошибка при обработке тикера %s: %s", sym, exc)
                
                self.socket_tasks.append(asyncio.create_task(read_socket(symbol, socket)))
            
            logger.info("WebSocket соединения запущены для %s", symbols)
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка при запуске WebSocket: %s", exc)
            raise
    
    async def stop(self) -> None:
        """Остановить WebSocket соединения."""
        self.is_running = False
        
        # Останавливаем задачи чтения сокетов
        for task in self.socket_tasks:
            task.cancel()
        for task in self.socket_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.socket_tasks.clear()

        # Закрываем все сокеты
        for symbol, socket in self.sockets:
            try:
                # Для async сокетов достаточно закрыть контекст
                if hasattr(socket, "close"):
                    await socket.close()
            except Exception as exc:  # noqa: BLE001
                logger.warning("Ошибка при закрытии сокета %s: %s", symbol, exc)
        
        self.sockets.clear()
        
        # Закрываем клиент
        if self.client:
            await self.client.close_connection()
        
        logger.info("WebSocket соединения остановлены")
    
    def set_price_callback(
        self,
        callback: Callable[[str, float, datetime], None],
    ) -> None:
        """
        Установить callback для обработки обновлений цен.
        
        Args:
            callback: Функция, которая будет вызвана при обновлении цены
                     Принимает: (symbol, price, timestamp)
        """
        self.price_callback = callback
    
    def get_last_price(self, symbol: str) -> Optional[float]:
        """
        Получить последнюю цену для символа.
        
        Args:
            symbol: Символ (например, "BTCUSDT")
        
        Returns:
            Последняя цена или None
        """
        return self.last_prices.get(symbol)
    
    async def get_historical_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 100,
    ) -> list:
        """
        Получить исторические свечи через REST API.
        
        Args:
            symbol: Символ (например, "BTCUSDT")
            interval: Интервал свечи ("1m", "5m", и т.д.)
            limit: Количество свеч
        
        Returns:
            Список свеч
        """
        await self._ensure_client()
        
        try:
            klines = await self.client.get_klines(
                symbol=symbol,
                interval=interval,
                limit=limit,
            )
            
            # Конвертируем в удобный формат
            formatted_klines = []
            for kline in klines:
                formatted_klines.append({
                    "timestamp": datetime.fromtimestamp(kline[0] / 1000),
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                })
            
            logger.info("Получено %s исторических свечей для %s", len(formatted_klines), symbol)
            return formatted_klines
        except BinanceAPIException as exc:
            logger.error("Ошибка Binance API при получении свечей: %s", exc)
            return []
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка при получении исторических свечей: %s", exc, exc_info=True)
            return []
