"""
Price Monitor - главный класс для мониторинга цен и расчета индикаторов.

Объединяет:
- Binance connector (WebSocket)
- Price Buffer (хранение в памяти)
- Индикаторы (RSI, MACD, ATR, SMA, Momentum)
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

from app.config import (
    CANDLE_COLORS,
    CANDLE_STREAK_TIMEFRAMES,
    CANDLE_LOG_INTERVAL_SECONDS,
    LOG_CANDLE_ANALYSIS,
    MIN_CANDLES_FOR_ANALYSIS,
    MONITOR_LOG_INTERVAL_SECONDS,
    MOMENTUM_RULE_ENABLE,
    MOMENTUM_RULE_SETS,
    MOMENTUM_TIMEFRAMES,
    SIGNAL_CHECK_INTERVAL_SECONDS,
    SIGNAL_COOLDOWN,
    settings,
)
from app.connectors.binance import BinanceConnector
from app.indicators.candle_analysis import CandleAnalyzer
from app.indicators.momentum import MultiTimeframeAnalyzer
from app.indicators.utils import calculate_atr, calculate_macd, calculate_rsi, calculate_sma
from app.storage.price_buffer import PriceBuffer

logger = logging.getLogger(__name__)


class PriceMonitor:
    """
    Главный класс для мониторинга цен и расчета индикаторов.
    
    Запускает:
    - WebSocket соединения для получения цен в реальном времени
    - Асинхронный цикл обновления индикаторов каждую секунду
    - Логирование метрик
    """
    
    def __init__(self):
        """Инициализация Price Monitor."""
        self.symbols = settings.binance_symbols or ["BTCUSDT", "ETHUSDT"]
        # Binance connector
        self.binance = BinanceConnector(
            api_key=settings.binance_api_key or None,
            api_secret=settings.binance_api_secret or None,
            symbols=self.symbols,
        )
        
        # Price buffers для каждого символа
        self.buffers: Dict[str, PriceBuffer] = {
            symbol: PriceBuffer(
                price_buffer_size=settings.price_buffer_size,
                kline_buffer_size_1m=settings.kline_buffer_size_1m,
                kline_buffer_size_5m=settings.kline_buffer_size_5m,
            )
            for symbol in self.symbols
        }
        
        # Устанавливаем callback для обновления цен
        self.binance.set_price_callback(self._on_price_update)
        
        # Флаг работы
        self.is_running = False
        self.update_task: Optional[asyncio.Task] = None
        self.kline_task: Optional[asyncio.Task] = None

        # Настройки сигналов
        self.last_signal_time: dict[str, datetime] = {}
        self.signal_cooldown: int = SIGNAL_COOLDOWN
        self.signal_check_interval: int = SIGNAL_CHECK_INTERVAL_SECONDS
        self.last_signal_check: Optional[datetime] = None
        self.latest_stats: dict[str, Dict] = {}
        self.last_candle_log_time: dict[str, datetime] = {}
        self.candle_log_interval: int = CANDLE_LOG_INTERVAL_SECONDS
        self.last_monitor_log_time: dict[str, datetime] = {}
        self.monitor_log_interval: int = MONITOR_LOG_INTERVAL_SECONDS
        
        logger.info("PriceMonitor инициализирован")
    
    def _on_price_update(self, symbol: str, price: float, timestamp: datetime) -> None:
        """
        Callback для обработки обновлений цен от WebSocket.
        
        Args:
            symbol: Символ (например, "BTCUSDT")
            price: Цена
            timestamp: Временная метка
        """
        if symbol in self.buffers:
            self.buffers[symbol].add_price(price, timestamp)
    
    async def start(self) -> None:
        """Запустить мониторинг цен."""
        try:
            # Загружаем исторические свечи при старте
            logger.info("Загрузка исторических свечей...")
            for symbol in self.symbols:
                # Загружаем последние 100 1m свеч
                klines = await self.binance.get_historical_klines(
                    symbol=symbol,
                    interval="1m",
                    limit=settings.kline_history_limit_1m,
                )
                
                # Добавляем в буфер
                for kline in klines:
                    self.buffers[symbol].add_kline(kline, "1m")
                    # Также добавляем цену закрытия как точку цены
                    self.buffers[symbol].add_price(
                        kline["close"],
                        kline["timestamp"],
                    )
                
                logger.info("Загружено %s исторических свечей для %s", len(klines), symbol)
            
            # Запускаем WebSocket соединения
            await self.binance.start()
            
            # Запускаем асинхронные задачи
            self.is_running = True
            self.kline_task = asyncio.create_task(self._listen_kline_updates())
            self.update_task = asyncio.create_task(self._update_loop())
            
            logger.info("PriceMonitor запущен")
        except Exception as exc:  # noqa: BLE001
            logger.error("Ошибка при запуске PriceMonitor: %s", exc)
            raise
    
    async def stop(self) -> None:
        """Остановить мониторинг цен."""
        self.is_running = False
        
        # Останавливаем задачу обновления
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass

        if self.kline_task:
            self.kline_task.cancel()
            try:
                await self.kline_task
            except asyncio.CancelledError:
                pass
        
        # Останавливаем Binance connector
        await self.binance.stop()
        
        logger.info("PriceMonitor остановлен")
    
    async def _update_loop(self) -> None:
        """Асинхронный цикл обновления индикаторов каждую секунду."""
        while self.is_running:
            try:
                # Обновляем индикаторы для каждого символа
                for symbol in self.symbols:
                    stats = self.get_stats(symbol)
                    if stats:
                        self.latest_stats[symbol] = stats
                        # Логируем метрики
                        self._log_stats(symbol, stats)

                # Раз в минуту проверяем и логируем текущий сигнал
                now = datetime.now()
                if (
                    self.last_signal_check is None
                    or (now - self.last_signal_check).total_seconds()
                    >= self.signal_check_interval
                ):
                    self.last_signal_check = now
                    if not MOMENTUM_RULE_ENABLE:
                        continue
                    for symbol in self.symbols:
                        stats = self.latest_stats.get(symbol)
                        if not stats:
                            continue
                        momentums = stats.get("momentums", {}) or {}
                        signal = self._check_multi_timeframe_signal(
                            symbol,
                            momentums,
                            {},
                            MOMENTUM_RULE_SETS,
                            respect_cooldown=False,
                            update_last=False,
                        )
                        if signal:
                            logger.info(
                                "CURRENT SIGNAL: %s → %s | Strength: %.3f%% | Momentums: %s",
                                symbol.upper(),
                                signal["direction"],
                                signal["strength"],
                                signal["momentums_used"],
                            )
                        else:
                            logger.info("CURRENT SIGNAL: %s → NONE", symbol.upper())
            except Exception as exc:  # noqa: BLE001
                logger.error("Ошибка в цикле обновления: %s", exc)
            
            # Ждем 1 секунду
            await asyncio.sleep(1)
    
    def _log_stats(self, symbol: str, stats: Dict) -> None:
        """
        Логировать статистику для символа.
        
        Args:
            symbol: Символ
            stats: Словарь со статистикой
        """
        price = stats.get("price", 0)
        momentums = stats.get("momentums", {}) or {}
        rsi = stats.get("rsi_14")
        macd = stats.get("macd")
        atr = stats.get("atr")
        candle_logs = stats.get("candle_logs", [])
        
        macd_str = (
            f"macd={macd.get('macd', 0):.4f}, "
            f"signal={macd.get('signal', 0):.4f}, "
            f"hist={macd.get('histogram', 0):.4f}"
            if macd
            else "N/A"
        )
        
        momentum_str = " | ".join(
            f"{tf}: {val:.3f}%" if val is not None else f"{tf}: N/A"
            for tf, val in momentums.items()
        )
        if not momentum_str:
            momentum_str = "N/A"

        now = datetime.now()
        last_log = self.last_monitor_log_time.get(symbol)
        should_log = (
            last_log is None
            or (now - last_log).total_seconds() >= self.monitor_log_interval
        )

        if should_log:
            self.last_monitor_log_time[symbol] = now
            logger.info(
                "%s: $%.2f | Momentums: %s | RSI: %s | MACD: {%s} | ATR: %s",
                symbol,
                price,
                momentum_str,
                f"{rsi:.1f}" if rsi is not None else "N/A",
                macd_str,
                f"{atr:.2f}" if atr is not None else "N/A",
            )

        if LOG_CANDLE_ANALYSIS and candle_logs:
            now = datetime.now()
            last_ts = self.last_candle_log_time.get(symbol)
            if last_ts and (now - last_ts).total_seconds() < self.candle_log_interval:
                return
            self.last_candle_log_time[symbol] = now
            logger.info("Candles: %s", " | ".join(candle_logs))
        
        # Сигналы с дебаунсингом (мультитаймфрейм)
        signal = stats.get("signal")
        if signal:
            logger.warning(
                "🚨 SIGNAL: %s → %s | Strength: %.3f%% | RSI: %s | Momentums: %s",
                symbol.upper(),
                signal["direction"],
                signal["strength"],
                f"{rsi:.1f}" if rsi is not None else "N/A",
                signal["momentums_used"],
            )

    def _check_multi_timeframe_signal(
        self,
        symbol: str,
        momentums: dict[str, float | None],
        thresholds: dict[str, float],
        rule_sets: list[dict[str, float]] | None = None,
        *,
        respect_cooldown: bool = True,
        update_last: bool = True,
    ) -> Optional[Dict]:
        """
        Проверяем сигнал по нескольким таймфреймам.

        Сигнал на UP, если:
        - все доступные momentum > их пороги
        - они совпадают по знаку
        - прошел cooldown с последнего сигнала
        """
        now = datetime.now()

        rule_sets = rule_sets or [thresholds]
        for rule in rule_sets:
            if not rule:
                continue

            rule_momentums: dict[str, float] = {}
            for tf, threshold in rule.items():
                val = momentums.get(tf)
                if val is None:
                    rule_momentums = {}
                    break
                rule_momentums[tf] = val

                if threshold > 0 and val < threshold:
                    rule_momentums = {}
                    break
                if threshold < 0 and val > threshold:
                    rule_momentums = {}
                    break
                if threshold == 0:
                    continue

            if not rule_momentums:
                continue

            all_positive = all(val > 0 for val in rule_momentums.values())
            all_negative = all(val < 0 for val in rule_momentums.values())
            if all_positive:
                direction = "UP"
            elif all_negative:
                direction = "DOWN"
            else:
                continue

            if respect_cooldown:
                last_ts = self.last_signal_time.get(symbol)
                if last_ts is not None:
                    if (now - last_ts).total_seconds() < self.signal_cooldown:
                        return None

            if update_last:
                self.last_signal_time[symbol] = now

            strength = sum(abs(val) for val in rule_momentums.values()) / len(rule_momentums)

            return {
                "direction": direction,
                "strength": strength,
                "momentums_used": rule_momentums,
                "timestamp": now,
                "rule": rule,
            }

        return None

    async def _listen_kline_updates(self) -> None:
        """
        Слушаем закрытие 1m свечей для всех символов
        и добавляем их в PriceBuffer для корректного RSI/MACD/ATR.
        """
        if not self.binance.socket_manager:
            logger.warning("BinanceSocketManager не инициализирован, пропускаем kline stream")
            return

        streams = [f"{symbol.lower()}@kline_1m" for symbol in self.symbols]
        socket = self.binance.socket_manager.multiplex_socket(streams)

        async with socket as stream:
            while self.is_running:
                msg = await stream.recv()
                if not msg:
                    continue

                data = msg.get("data", msg)
                kline = data.get("k")
                if not kline:
                    continue

                if kline.get("x"):
                    symbol = data.get("s", "").upper()
                    if symbol not in self.buffers:
                        continue

                    kline_data = {
                        "timestamp": datetime.fromtimestamp(kline["t"] / 1000),
                        "open": float(kline["o"]),
                        "high": float(kline["h"]),
                        "low": float(kline["l"]),
                        "close": float(kline["c"]),
                        "volume": float(kline["v"]),
                    }
                    self.buffers[symbol].add_kline(kline_data, interval="1m")
                    logger.info(
                        "New 1m kline: %s close=%.2f",
                        symbol,
                        kline_data["close"],
                    )

    def _calculate_indicators(self, symbol: str) -> Dict:
        buffer = self.buffers.get(symbol)
        if not buffer:
            return {}

        latest_price = buffer.get_latest_price()
        if latest_price is None:
            return {}

        df = buffer.to_dataframe("1m")

        momentums = MultiTimeframeAnalyzer.calculate_all_momentums(
            df_1m=df,
            price_buffer=buffer,
            timeframes=MOMENTUM_TIMEFRAMES,
            symbol=symbol,
        )

        candle_analysis: dict[str, object] = {}
        candle_logs: list[str] = []

        if df is None or df.empty:
            signal = self._check_multi_timeframe_signal(
                symbol,
                momentums,
                {},
                MOMENTUM_RULE_SETS,
            )
            return {
                "price": latest_price,
                "momentums": momentums,
                "rsi_14": None,
                "macd": None,
                "atr": None,
                "sma_5": None,
                "candles": candle_analysis,
                "candle_logs": candle_logs,
                "timestamp": datetime.now(),
                "signal": signal,
            }

        close_prices = df["close"]

        rsi = None
        if len(df) >= settings.rsi_period:
            rsi = calculate_rsi(close_prices, period=settings.rsi_period)

        macd = None
        if len(df) >= settings.macd_slow:
            macd = calculate_macd(
                close_prices,
                fast=settings.macd_fast,
                slow=settings.macd_slow,
                signal=settings.macd_signal,
            )

        atr = None
        if len(df) >= settings.atr_period:
            atr = calculate_atr(
                df["high"],
                df["low"],
                df["close"],
                period=settings.atr_period,
            )

        sma = calculate_sma(close_prices, period=settings.sma_period)

        if LOG_CANDLE_ANALYSIS:
            if "1m" in CANDLE_STREAK_TIMEFRAMES:
                analysis_1m = CandleAnalyzer.analyze_df(
                    df, min_candles=MIN_CANDLES_FOR_ANALYSIS
                )
                if analysis_1m:
                    candle_analysis["1m"] = analysis_1m
                    candle_logs.append(
                        CandleAnalyzer.format_candle_log(analysis_1m, "1m", CANDLE_COLORS)
                    )

            if "5m" in CANDLE_STREAK_TIMEFRAMES:
                df_5m = MultiTimeframeAnalyzer.resample_to_5min(df)
                analysis_5m = CandleAnalyzer.analyze_df(
                    df_5m, min_candles=MIN_CANDLES_FOR_ANALYSIS
                )
                if analysis_5m:
                    candle_analysis["5m"] = analysis_5m
                    candle_logs.append(
                        CandleAnalyzer.format_candle_log(analysis_5m, "5m", CANDLE_COLORS)
                    )

            if "15m" in CANDLE_STREAK_TIMEFRAMES:
                df_15m = MultiTimeframeAnalyzer.resample_to_15min(df)
                analysis_15m = CandleAnalyzer.analyze_df(
                    df_15m, min_candles=MIN_CANDLES_FOR_ANALYSIS
                )
                if analysis_15m:
                    candle_analysis["15m"] = analysis_15m
                    candle_logs.append(
                        CandleAnalyzer.format_candle_log(
                            analysis_15m, "15m", CANDLE_COLORS
                        )
                    )

            if "1h" in CANDLE_STREAK_TIMEFRAMES:
                df_1h = MultiTimeframeAnalyzer.resample_to_hourly(df)
                analysis_1h = CandleAnalyzer.analyze_df(
                    df_1h, min_candles=MIN_CANDLES_FOR_ANALYSIS
                )
                if analysis_1h:
                    candle_analysis["1h"] = analysis_1h
                    candle_logs.append(
                        CandleAnalyzer.format_candle_log(analysis_1h, "1h", CANDLE_COLORS)
                    )

        signal = self._check_multi_timeframe_signal(
            symbol,
            momentums,
            {},
            MOMENTUM_RULE_SETS,
        )

        return {
            "price": latest_price,
            "momentums": momentums,
            "rsi_14": rsi,
            "macd": macd,
            "atr": atr,
            "sma_5": sma,
            "candles": candle_analysis,
            "candle_logs": candle_logs,
            "timestamp": datetime.now(),
            "signal": signal,
        }
    
    def get_stats(self, symbol: str) -> Optional[Dict]:
        """
        Получить текущие индикаторы для символа.
        
        Args:
            symbol: Символ (например, "BTCUSDT")
        
        Returns:
            Словарь со статистикой:
            {
                'price': float,
                'momentums': dict[str, float | None],
                'rsi_14': float,
                'macd': {'macd': float, 'signal': float, 'histogram': float},
                'atr': float,
                'sma_5': float,
                'timestamp': datetime,
                'candles': dict
            }
        """
        if symbol not in self.buffers:
            return None
        
        return self._calculate_indicators(symbol)

    def get_current_signal(self, symbol: str) -> Optional[Dict]:
        """
        Рассчитать текущий сигнал без учета cooldown.
        Используется для принятия решения о входе в сделку.
        """
        stats = self.latest_stats.get(symbol) or self._calculate_indicators(symbol)
        if not stats:
            return None
        momentums = stats.get("momentums", {}) or {}
        return self._check_multi_timeframe_signal(
            symbol,
            momentums,
            {},
            MOMENTUM_RULE_SETS,
            respect_cooldown=False,
            update_last=False,
        )
    
    def get_price(self, symbol: str) -> Optional[float]:
        """
        Получить последнюю цену для символа.
        
        Args:
            symbol: Символ (например, "BTCUSDT")
        
        Returns:
            Последняя цена или None
        """
        return self.binance.get_last_price(symbol)
