import json
import logging
import os
from dataclasses import dataclass

from dotenv import load_dotenv


# Загрузка переменных окружения из .env (если есть)
load_dotenv()

logger = logging.getLogger(__name__)


def _parse_momentum_config() -> list[tuple[int, str]]:
    """Парсим momentum-конфиг из .env или используем дефолты."""
    try:
        tf_json = os.getenv("MOMENTUM_TIMEFRAMES_JSON")
        if tf_json:
            raw = json.loads(tf_json)
            return [(int(v), str(t)) for v, t in raw]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to parse MOMENTUM_TIMEFRAMES_JSON: %s", exc)

    return [
        (15, "minute"),
        (5, "minute"),
        (1, "minute"),
        (10, "second"),
    ]


def _parse_momentum_rule_sets() -> list[dict[str, float]]:
    """
    Парсим наборы правил для сигналов momentum.
    Формат: список словарей, где ключи — таймфреймы ("15m", "5m", "1m", "10s"),
    значения — пороги (float).
    """
    raw = os.getenv("MOMENTUM_RULE_SETS_JSON")
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                rule_sets: list[dict[str, float]] = []
                for item in data:
                    if isinstance(item, dict):
                        rule_sets.append({str(k): float(v) for k, v in item.items()})
                if rule_sets:
                    return rule_sets
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse MOMENTUM_RULE_SETS_JSON: %s", exc)

    return [
        {"15m": -0.001},
        {"15m":  0.001,},
        {"15m": -0.428, "5m": -0.259, "1m": -0.069, "10s": 0.068}
    ]


def _parse_bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _parse_candle_timeframes() -> list[str]:
    """Парсим таймфреймы для анализа свечей."""
    raw = os.getenv("CANDLE_STREAK_TIMEFRAMES")
    if raw:
        try:
            data = json.loads(raw)
            return [str(item) for item in data]
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse CANDLE_STREAK_TIMEFRAMES: %s", exc)
    return ["1m", "5m", "15m", "1h"]


def _parse_candle_colors() -> dict[str, str]:
    raw = os.getenv("CANDLE_COLORS_JSON")
    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to parse CANDLE_COLORS_JSON: %s", exc)
    return {
        "green": "🟢",
        "red": "🔴",
    }


# Momentum timeframes (в минутах и секундах)
# Формат: список кортежей (значение, тип), где тип = 'minute' или 'second'
MOMENTUM_TIMEFRAMES = _parse_momentum_config()

# Наборы правил для сигналов (OR по наборам)
MOMENTUM_RULE_SETS = _parse_momentum_rule_sets()

# Пример для .env:
# MOMENTUM_RULE_SETS_JSON='[{"15m": -0.428, "5m": -0.259, "1m": -0.069}, {"15m": -0.428, "5m": -0.259, "1m": -0.069, "10s": 0.068}]'

# Для логирования: минимальный промежуток между сигналами (секунды)
SIGNAL_COOLDOWN = int(os.getenv("SIGNAL_COOLDOWN", "3"))

# Как часто проверять и выводить текущий сигнал (секунды)
SIGNAL_CHECK_INTERVAL_SECONDS = int(os.getenv("SIGNAL_CHECK_INTERVAL_SECONDS", "10"))

# Как часто проверять вход в сделку (секунды)
SIGNAL_ENTRY_CHECK_SECONDS = float(os.getenv("SIGNAL_ENTRY_CHECK_SECONDS", "1"))

# Включать проверку правил momentum перед входом
MOMENTUM_RULE_ENABLE = _parse_bool_env("MOMENTUM_RULE_ENABLE", False)

# Проверять выгодность первой покупки по ask
BUY_MORE_PROFITABLE_FIRST_ENABLE = _parse_bool_env("BUY_MORE_PROFITABLE_FIRST_ENABLE", True )
BUY_MORE_PROFITABLE_FIRST_MAX_ASK = float(os.getenv("BUY_MORE_PROFITABLE_FIRST_MAX_ASK", "0.49"))
FIRST_ENTRY_MINUTES_TO_CLOSE_MIN = float(os.getenv("FIRST_ENTRY_MINUTES_TO_CLOSE_MIN", "0"))  # 0 = off
FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN = float(os.getenv("FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN", "2"))  # 0 = off

# === POST-ENTRY RULES ===
POST_ENTRY_RULES_ENABLE = _parse_bool_env("POST_ENTRY_RULES_ENABLE", True)
POST_ENTRY_CHANGE_PCT_MAX = float(os.getenv("POST_ENTRY_CHANGE_PCT_MAX", "0.01"))  # N максимальный процент отклонения цены для закупки противоположного направления
POST_ENTRY_CHANGE_PCT_ADD_SAME_MIN = float(os.getenv("POST_ENTRY_CHANGE_PCT_ADD_SAME_MIN", "0.1"))  # L максимальный процент отклонения цены для докупа
POST_ENTRY_MAX_ASK = float(os.getenv("POST_ENTRY_MAX_ASK", "0.32"))  # X максимальная цена для докупа
POST_ENTRY_ADD_USD = float(os.getenv("POST_ENTRY_ADD_USD", "30"))  # Y сумма для закупки противоположного направления
POST_ENTRY_EXIT_BID_TARGET = float(os.getenv("POST_ENTRY_EXIT_BID_TARGET", "0.25"))  # P условие выхода где P цена стоп лосса
POST_ENTRY_EXIT_CHANGE_PCT_MIN = float(os.getenv("POST_ENTRY_EXIT_CHANGE_PCT_MIN", "0.2"))  # J условие выхода где J процент отколнения цены 
POST_ENTRY_MINUTES_TO_CLOSE_MIN = float(os.getenv("POST_ENTRY_MINUTES_TO_CLOSE_MIN", "3"))  # M1 Минуты до закрытия сделки для закупки противоположного направления
POST_ENTRY_MINUTES_TO_CLOSE_MAX = float(os.getenv("POST_ENTRY_MINUTES_TO_CLOSE_MAX", "3"))  # M2 Минуты до закрытия сделки для докупа позиции

# === POST-ENTRY RULES (legacy placeholders) ===
LEGACY_POST_ENTRY_BUY_USDC = float(os.getenv("POST_ENTRY_BUY_USDC", "20"))
LEGACY_POST_ENTRY_EXIT_PRICE = float(os.getenv("POST_ENTRY_EXIT_PRICE", "0.3"))
LEGACY_POST_ENTRY_EXIT_CHANGE_PCT = float(os.getenv("POST_ENTRY_EXIT_CHANGE_PCT", "0.1"))

# Как часто логировать отклонение цены от price_to_beat (секунды)
PRICE_TO_BEAT_LOG_SECONDS = float(os.getenv("PRICE_TO_BEAT_LOG_SECONDS", "10"))

# === CANDLE COLOR & STREAK ANALYSIS ===
LOG_CANDLE_ANALYSIS = _parse_bool_env("LOG_CANDLE_ANALYSIS", True)
CANDLE_STREAK_TIMEFRAMES = _parse_candle_timeframes()
MIN_CANDLES_FOR_ANALYSIS = int(os.getenv("MIN_CANDLES_FOR_ANALYSIS", "10"))
CANDLE_COLORS = _parse_candle_colors()
CANDLE_LOG_INTERVAL_SECONDS = int(os.getenv("CANDLE_LOG_INTERVAL_SECONDS", "700"))

# === PRICE MONITOR LOGGING ===
# Как часто писать полный лог монитора (секунды)
MONITOR_LOG_INTERVAL_SECONDS = int(os.getenv("MONITOR_LOG_INTERVAL_SECONDS", "50"))

# === SIMULATION MODE ===
SIMULATION_MODE = _parse_bool_env("SIMULATION_MODE", True)
SIMULATION_INITIAL_BALANCE = float(os.getenv("SIMULATION_INITIAL_BALANCE", "1000"))
SIMULATION_POSITION_SIZE_PCT = float(os.getenv("SIMULATION_POSITION_SIZE_PCT", "0.1"))
SIMULATION_TRADING_FEE_PCT = float(os.getenv("SIMULATION_TRADING_FEE_PCT", "0.02"))


@dataclass
class Settings:
    """Конфигурация приложения, загружаемая из переменных окружения."""

    polymarket_private_key: str
    polymarket_wallet_address: str
    polymarket_api_url: str
    
    # Binance API (опциональны для публичных данных)
    binance_api_key: str = ""
    binance_api_secret: str = ""
    binance_symbols: list[str] | None = None
    
    # Параметры индикаторов
    momentum_threshold: float = 0.08  # % за 10 сек для сигнала
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9
    atr_period: int = 14
    sma_period: int = 5
    
    # Размеры буферов
    price_buffer_size: int = 3600  # 1 час в секундах
    kline_buffer_size_1m: int = 1440  # 24 часа 1m свеч
    kline_buffer_size_5m: int = 288  # 24 часа 5m свеч
    kline_history_limit_1m: int = 150  # сколько 1m свеч загружать на старте

    @classmethod
    def from_env(cls) -> "Settings":
        private_key = os.getenv("POLYMARKET_PRIVATE_KEY") or ""
        wallet_address = os.getenv("POLYMARKET_WALLET_ADDRESS") or ""
        api_url = os.getenv("POLYMARKET_API_URL", "https://clob.polymarket.com")

        if not private_key:
            raise RuntimeError(
                "POLYMARKET_PRIVATE_KEY не задан. Установите его в .env или переменных окружения."
            )
        if not wallet_address:
            raise RuntimeError(
                "POLYMARKET_WALLET_ADDRESS не задан. Установите его в .env или переменных окружения."
            )

        # Binance API (опциональны)
        binance_api_key = os.getenv("BINANCE_API_KEY", "")
        binance_api_secret = os.getenv("BINANCE_API_SECRET", "")
        symbols_raw = os.getenv("BINANCE_SYMBOLS", "BTCUSDT")
        binance_symbols = [
            s.strip().upper() for s in symbols_raw.split(",") if s.strip()
        ]
        
        # Параметры индикаторов (можно переопределить через .env)
        momentum_threshold = float(os.getenv("MOMENTUM_THRESHOLD", "0.08"))
        rsi_period = int(os.getenv("RSI_PERIOD", "14"))
        macd_fast = int(os.getenv("MACD_FAST", "12"))
        macd_slow = int(os.getenv("MACD_SLOW", "26"))
        macd_signal = int(os.getenv("MACD_SIGNAL", "9"))
        atr_period = int(os.getenv("ATR_PERIOD", "14"))
        sma_period = int(os.getenv("SMA_PERIOD", "5"))
        
        # Размеры буферов
        price_buffer_size = int(os.getenv("PRICE_BUFFER_SIZE", "3600"))
        kline_buffer_size_1m = int(os.getenv("KLINE_BUFFER_SIZE_1M", "1440"))
        kline_buffer_size_5m = int(os.getenv("KLINE_BUFFER_SIZE_5M", "288"))
        kline_history_limit_1m = int(os.getenv("KLINE_HISTORY_LIMIT_1M", "150"))
   
        
        return cls(
            polymarket_private_key=private_key,
            polymarket_wallet_address=wallet_address,
            polymarket_api_url=api_url,
            binance_api_key=binance_api_key,
            binance_api_secret=binance_api_secret,
            binance_symbols=binance_symbols,
            momentum_threshold=momentum_threshold,
            rsi_period=rsi_period,
            macd_fast=macd_fast,
            macd_slow=macd_slow,
            macd_signal=macd_signal,
            atr_period=atr_period,
            sma_period=sma_period,
            price_buffer_size=price_buffer_size,
            kline_buffer_size_1m=kline_buffer_size_1m,
            kline_buffer_size_5m=kline_buffer_size_5m,
            kline_history_limit_1m=kline_history_limit_1m,
        )


settings = Settings.from_env()

