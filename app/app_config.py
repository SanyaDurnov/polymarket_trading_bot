"""
Настройки приложения для работы с Polymarket API.

Этот файл содержит параметры конфигурации, которые можно изменять
без изменения кода. Для чувствительных данных (приватные ключи)
используйте файл .env.
"""

from dataclasses import dataclass, field


@dataclass
class AppConfig:
    """Конфигурация приложения."""

    # Автоматическая генерация фильтров на основе текущего времени
    # Если True, фильтры генерируются автоматически на основе текущей даты и времени (ET)
    # Если False, используются фильтры из market_search_filter
    auto_generate_filters: bool = True
    
    # Список монет для автоматической генерации фильтров
    # Фильтры будут созданы для каждой монеты из этого списка
    coins: list[str] = field(default_factory=lambda: [
        "Bitcoin",
        "Ethereum",
        "Solana"
    ])
    
    # Параметры фильтрации для поиска рынков
    # Используется в Gamma API и для клиентской фильтрации
    # Может быть строкой или списком строк для поиска по нескольким фильтрам
    # Используется только если auto_generate_filters = False
    # Примеры:
    #   market_search_filter = "Bitcoin Up or Down - January 12, 10:45AM-11:00AM ET"
    #   market_search_filter = ["Bitcoin Up or Down - January 12, 10:45AM-11:00AM ET", "Bitcoin Up or Down - January 12, 11:00AM-11:15AM ET"]
    market_search_filter: str | list[str] = field(default_factory=lambda: [
        "Ethereum Up or Down - January 13, 8:45AM-9:00AM ET",
        "Bitcoin Up or Down - January 13, 9-10AM ET",
        "Bitcoin Up or Down - January 13, 9:15-9:30AM ET"
    ])
    
    # Выводить список сгенерированных фильтров в консоль при тестовом прогоне
    print_filters_on_test: bool = True
    
    # Выводить первые 10 рынков в консоль при тестовом прогоне
    print_markets_on_test: bool = True
    
    # Выводить информацию об аккаунте в консоль при тестовом прогоне
    print_account_info_on_test: bool = False
    
    # Использовать Gamma API по умолчанию для получения рынков
    use_gamma_api: bool = True
    
    # Максимальное количество рынков для возврата (0 = без ограничений)
    max_markets_limit: int = 30000
    
    # Параметры для Gamma API
    gamma_api_active_only: bool = True
    gamma_api_closed: bool = False
    # Тег для фильтрации рынков по категории (по названию или ID)
    # Можно указать название тега (например, "Crypto", "Politics") или None для отключения фильтрации
    # Если указано название, код автоматически найдет соответствующий tag_id
    # Примеры: "Crypto", "Politics", "Sports games", None
    gamma_api_tag: str | None = 'crypto'
    # Альтернативно можно указать tag_id напрямую (число)
    # Если задан gamma_api_tag, этот параметр игнорируется
    # Примеры: 2 (Crypto), 21 (Politics), 100639 (Sports games)
    gamma_api_tag_id: int | None = None

    # Как часто обновлять кэш рынков (минуты)
    markets_update_interval_minutes: int = 15
    
    # Параметры для CLOB API
    clob_api_max_pages: int = 60  # Максимальное количество страниц для пагинации (60 * 500 = 30000 рынков)
    
    # Фильтрация рынков по времени начала
    # Удалять рынки, которые начались больше чем N минут назад (0 = не фильтровать)
    # Время начала извлекается из названия рынка
    # Примеры: "Bitcoin Up or Down - January 13, 10:30AM-10:45AM ET" -> 10:30AM
    #          "Ethereum Up or Down - January 13, 10AM ET" -> 10:00AM
    filter_markets_started_minutes_ago: int = 5  # 0 = не фильтровать, > 0 = фильтровать
    
    # Фильтрация рынков по времени начала (удалять те, что начнутся слишком далеко)
    # Удалять рынки, которые начнутся позже чем через N минут (0 = не фильтровать)
    # Например, если рынок начинается в 11:00, а N = 5:
    # - В 10:54: до начала 6 минут > 5 -> удаляем (слишком далеко)
    # - В 10:56: до начала 4 минуты <= 5 -> оставляем (достаточно близко)
    filter_markets_starting_within_minutes: int = 10  # 0 = не фильтровать, > 0 = фильтровать

    # === CLOB ORDERBOOK WEBSOCKET ===
    polymarket_ws_enabled: bool = False
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    polymarket_ws_urls: list[str] = field(default_factory=lambda: [
        "wss://ws-subscriptions-clob.polymarket.com/ws/",
    ])
    polymarket_ws_ping_interval: int = 20
    polymarket_ws_ping_timeout: int = 10
    polymarket_ws_close_timeout: int = 10
    polymarket_ws_compression: str | None = None
    polymarket_ws_reconnect_seconds: int = 2
    polymarket_ws_stale_seconds: int = 5
    # Подписка на маркет: {"type": "market", "assets_ids": [market_id]}
    polymarket_ws_subscribe_type: str = "market"
    polymarket_ws_subscribe_assets_key: str = "assets_ids"
    polymarket_ws_origin: str | None = None
    polymarket_ws_headers: dict[str, str] = field(default_factory=lambda: {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        ),
        "Origin": "https://polymarket.com",
        "Sec-WebSocket-Version": "13",
        "Sec-WebSocket-Extensions": "permessage-deflate; client_max_window_bits",
    })
    polymarket_orderbook_debug_enabled: bool = True
    polymarket_orderbook_debug_interval_seconds: int = 1
    enable_spread: bool = True
    lite_logs: bool = True
    post_entry_position_log_interval_seconds: int = 5
    post_entry_action_log_interval_seconds: int = 10
    orderbook_debug_log_interval_seconds: int = 15
    spread_log_interval_seconds: int = 2000
    post_entry_skip_logs_enabled: bool = True
    post_entry_rule_check_logs_enabled: bool = True
    post_entry_rule_check_log_interval_seconds: int = 10


# Глобальный экземпляр конфигурации
app_config = AppConfig()
