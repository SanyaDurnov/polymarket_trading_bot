# Binance Real-Time Price Monitoring

## Описание

Модуль для мониторинга цен Binance в реальном времени и расчета технических индикаторов.

## Компоненты

### 1. BinanceConnector (`app/connectors/binance.py`)
- WebSocket подключение к Binance для получения цен BTCUSDT и ETHUSDT в реальном времени
- Загрузка исторических свечей через REST API

### 2. PriceBuffer (`app/storage/price_buffer.py`)
- Хранение цен и свечей в памяти (collections.deque)
- Поддержка буферов для 1m и 5m свеч

### 3. Индикаторы (`app/indicators/`)
- **Momentum**: процентное изменение цены за период
- **RSI**: Relative Strength Index
- **MACD**: Moving Average Convergence Divergence
- **ATR**: Average True Range
- **SMA**: Simple Moving Average

### 4. PriceMonitor (`app/connectors/price_monitor.py`)
- Главный класс, объединяющий все компоненты
- Асинхронный цикл обновления индикаторов каждую секунду
- Логирование метрик

## Установка зависимостей

```bash
pip install -r requirements.txt
```

## Настройка

Добавьте в `.env` (опционально, для публичных данных не требуется):
```
BINANCE_API_KEY=your_api_key
BINANCE_API_SECRET=your_api_secret
```

Параметры индикаторов можно настроить в `app/config.py` или через переменные окружения:
- `MOMENTUM_THRESHOLD` - порог для сигнала (по умолчанию 0.08)
- `RSI_PERIOD` - период RSI (по умолчанию 14)
- `MACD_FAST`, `MACD_SLOW`, `MACD_SIGNAL` - параметры MACD
- `ATR_PERIOD` - период ATR (по умолчанию 14)
- `PRICE_BUFFER_SIZE` - размер буфера цен (по умолчанию 3600)
- `KLINE_BUFFER_SIZE_1M` - размер буфера 1m свеч (по умолчанию 1440)

## Использование

### Запуск мониторинга цен

```bash
python -m app.main monitor
# или
python -m app.main price
```

### Программное использование

```python
import asyncio
from app.connectors.price_monitor import PriceMonitor

async def main():
    monitor = PriceMonitor()
    await monitor.start()
    
    # Получить статистику
    stats = monitor.get_stats("BTCUSDT")
    print(stats)
    
    # Остановить мониторинг
    await monitor.stop()

asyncio.run(main())
```

## Формат логов

Каждую секунду выводится:
```
BTCUSDT: $91000.50 | Momentum: 0.123% | RSI: 65.5 | MACD: {macd=0.1234, signal=0.5678, hist=-0.4444} | ATR: 123.45
```

При превышении порога momentum:
```
[WARNING] SIGNAL: UP (strength: 0.145%)
```

## Структура данных статистики

```python
{
    'price': float,              # Текущая цена
    'momentum_10s': float,       # Momentum за 10 секунд (%)
    'rsi_14': float,             # RSI (14 период)
    'macd': {                    # MACD
        'macd': float,
        'signal': float,
        'histogram': float
    },
    'atr': float,                # ATR
    'sma_5': float,              # SMA (5 период)
    'timestamp': datetime         # Временная метка
}
```

## Примечания

- WebSocket используется для минимальной задержки получения цен
- Все индикаторы рассчитываются в памяти, без обращения к БД
- Код готов к production использованию: обработка ошибок, логирование, graceful shutdown
- Для остановки используйте Ctrl+C
