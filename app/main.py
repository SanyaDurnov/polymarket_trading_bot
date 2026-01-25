import asyncio
import logging
import signal
import sys
from pprint import pprint

from app.app_config import app_config
from app.config import settings
from app.connectors.price_monitor import PriceMonitor
from app.polymarket_client import PolymarketClient, check_geoblock
from app.trading.orchestrator import TradingOrchestrator


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def choose_market(client: PolymarketClient) -> str | None:
    # Используем фильтр из настроек
    markets = client.get_markets()
    if not markets:
        logger.info("Подходящие рынки не найдены (фильтр: '%s').", app_config.market_search_filter)
        logger.info("Попробуйте изменить параметр 'market_search_filter' в app/app_config.py")
        return None

    logger.info("Найдено %s рынков:", len(markets))
    for idx, m in enumerate(markets):
        market_id = m.get("id", "N/A")
        title = m.get("title") or m.get("question") or m.get("name") or "N/A"
        logger.info("%s) %s [id=%s]", idx, title, market_id)

    while True:
        raw = input("Введите индекс рынка для просмотра orderbook (или пусто для выхода): ").strip()
        if not raw:
            return None
        try:
            index = int(raw)
            if 0 <= index < len(markets):
                return str(markets[index].get("id"))
            logger.info("Неверный индекс. Допустимые значения: 0..%s", len(markets) - 1)
        except ValueError:
            logger.info("Пожалуйста, введите целое число.")


def show_orderbook(client: PolymarketClient, market_id: str) -> None:
    logger.info("Запрос orderbook для market_id=%s ...", market_id)
    orderbook = client.get_orderbook(market_id)
    if orderbook is None:
        logger.info("Не удалось получить orderbook.")
        return

    logger.info("Orderbook для market_id=%s:", market_id)
    pprint(orderbook)


def confirm(prompt: str) -> bool:
    ans = input(f"{prompt} (y/N): ").strip().lower()
    return ans in {"y", "yes"}


def interactive_trade(client: PolymarketClient, market_id: str) -> None:
    action = input("Выберите действие: 'buy', 'sell' или пусто для выхода: ").strip().lower()
    if action not in {"buy", "sell"}:
        logger.info("Действие не выбрано, выходим без сделок.")
        return

    # Опция dry-run
    dry_run_choice = input("Использовать dry-run режим (только симуляция, без реальной отправки)? (y/N): ").strip().lower()
    use_dry_run = dry_run_choice in {"y", "yes"}

    try:
        outcome_raw = input("Укажите исход (целое число, например 0 или 1): ").strip()
        outcome = int(outcome_raw)
    except ValueError:
        logger.info("Неверное значение outcome, ожидается целое число.")
        return

    try:
        amount_raw = input("Сумма в USDC (например 10): ").strip()
        amount_usdc = float(amount_raw)
    except ValueError:
        logger.info("Неверное значение суммы.")
        return

    try:
        price_raw = input("Лимитная цена (например 0.5): ").strip()
        price = float(price_raw)
    except ValueError:
        logger.info("Неверное значение цены.")
        return

    logger.info(
        "Планируется %s%s: market_id=%s, outcome=%s, amount_usdc=%s, price=%s",
        action,
        " [DRY RUN]" if use_dry_run else "",
        market_id,
        outcome,
        amount_usdc,
        price,
    )

    if not use_dry_run and not confirm("Подтвердить создание РЕАЛЬНОГО ордера?"):
        logger.info("Операция отменена пользователем, ордер не будет создан.")
        return

    if action == "buy":
        order = client.buy_outcome(market_id, outcome, amount_usdc, price, dry_run=use_dry_run)
    else:
        order = client.sell_outcome(market_id, outcome, amount_usdc, price, dry_run=use_dry_run)

    if order is None:
        logger.info("Не удалось создать ордер, подробности в логах.")
    else:
        if use_dry_run:
            logger.info("✓ Dry-run ордер успешно симулирован:")
        else:
            logger.info("✓ Ордер успешно создан:")
        pprint(order)


def test_run() -> None:
    """
    Комплексный тестовый прогон для проверки подключения и базовых операций.
    """
    # Включаем DEBUG логирование для тестового прогона
    logging.getLogger("app.polymarket_client").setLevel(logging.DEBUG)
    
    logger.info("=" * 60)
    logger.info("ТЕСТОВЫЙ ПРОГОН: Проверка подключения к Polymarket CLOB API")
    logger.info("=" * 60)

    # 1. Проверка конфигурации
    logger.info("\n[1/5] Проверка конфигурации...")
    logger.info("API URL: %s", settings.polymarket_api_url)
    logger.info("Wallet Address: %s", settings.polymarket_wallet_address[:10] + "..." if len(settings.polymarket_wallet_address) > 10 else settings.polymarket_wallet_address)
    logger.info("Private Key: %s", "***" if settings.polymarket_private_key else "НЕ ЗАДАН")

    # 2. Инициализация клиента
    logger.info("\n[2/5] Инициализация Polymarket клиента...")
    try:
        client = PolymarketClient()
        logger.info("✓ Клиент успешно инициализирован")
    except Exception as exc:  # noqa: BLE001
        logger.error("✗ Не удалось инициализировать клиента: %s", exc)
        return

    # Обновляем рынки при тестовом запуске
    logger.info("\nОбновление рынков при тестовом запуске...")
    client.check_and_update_markets(force=True)

    # 3. Проверка подключения
    logger.info("\n[3/5] Проверка подключения к API...")
    connection_result = client.test_connection()
    if connection_result["client_initialized"]:
        logger.info("✓ Клиент инициализирован")
    if connection_result["api_creds_set"]:
        logger.info("✓ API credentials установлены")
    if connection_result["markets_accessible"]:
        logger.info("✓ Доступ к рынкам: найдено %s рынков", connection_result["markets_count"])
    else:
        logger.warning("⚠ Рынки недоступны или не найдены")
    if connection_result["errors"]:
        logger.warning("⚠ Обнаружены ошибки:")
        for error in connection_result["errors"]:
            logger.warning("  - %s", error)

    # 4. Получение информации об аккаунте и балансе
    if app_config.print_account_info_on_test:
        logger.info("\n[4/5] Получение информации об аккаунте...")
        account_info = client.get_account_info()
        if account_info:
            logger.info("✓ Информация об аккаунте получена:")
            pprint(account_info)
        else:
            logger.warning("⚠ Не удалось получить информацию об аккаунте")

        balance = client.get_balance()
        if balance:
            logger.info("✓ Баланс получен:")
            pprint(balance)
        else:
            logger.warning("⚠ Не удалось получить баланс (метод может быть недоступен в этой версии клиента)")
    else:
        logger.info("\n[4/5] Получение информации об аккаунте... (пропущено, print_account_info_on_test = False)")

    # 5. Тест получения рынков и orderbook
    logger.info("\n[5/5] Тест получения рынков и orderbook...")
    
    # Получаем фильтры (автоматически сгенерированные или из конфига)
    from app.filter_generator import generate_filters
    
    filter_display = ""  # Инициализируем переменную для отображения
    filter_from_config = None  # Инициализируем для использования в сообщениях об ошибках
    
    if app_config.auto_generate_filters:
        filters = generate_filters(app_config.coins)
        filter_display = f"{len(filters)} автоматически сгенерированных фильтров"
        logger.info("Используем автоматически сгенерированные фильтры: %s", filter_display)
        has_filters = len(filters) > 0
        
        # Выводим список фильтров, если включено в конфиге
        if app_config.print_filters_on_test and filters:
            logger.info("\n📋 Список сгенерированных фильтров:")
            for idx, filter_str in enumerate(filters, 1):
                logger.info("  %s. %s", idx, filter_str)
    else:
        filter_from_config = app_config.market_search_filter
        
        # Обрабатываем как строку, так и список
        if isinstance(filter_from_config, list):
            filter_display = f"{len(filter_from_config)} фильтров: {filter_from_config[:3]}{'...' if len(filter_from_config) > 3 else ''}"
            logger.info("Используем фильтры из app_config: %s", filter_display)
            has_filters = len(filter_from_config) > 0
        else:
            filter_display = str(filter_from_config)
            logger.info("Используем фильтр из app_config: '%s'", filter_display)
            has_filters = bool(filter_from_config and filter_from_config.strip())
    
    if not has_filters:
        logger.warning("⚠ Фильтр в app_config пустой! Установите 'market_search_filter' в app/app_config.py или включите 'auto_generate_filters'")
        logger.info("Пропускаем получение рынков.")
        markets = []
    else:
        # Получаем рынки (будет использована автоматическая генерация или фильтры из конфига)
        markets = client.get_markets()  # Использует auto_generate_filters или market_search_filter
        logger.info("Получено рынков с фильтром '%s': %s", filter_display, len(markets) if markets else 0)
        
        # Выводим первые 10 полученных рынков, если включено в конфиге
        if markets and app_config.print_markets_on_test:
            markets_to_show = markets[:10]  # Первые 10
            logger.info("\n📋 Первые 10 рынков (из %s полученных):", len(markets))
            for idx, market in enumerate(markets_to_show, 1):
                market_id = str(market.get("id", "N/A"))
                title = market.get("title") or market.get("question") or market.get("name") or "N/A"
                logger.info("  %s. [ID: %s] %s", idx, market_id, title)
        else:
            logger.warning("⚠ Рынки не найдены с фильтром '%s'", filter_display)
            logger.info("Попробуйте изменить 'market_search_filter' в app/app_config.py или настройки автогенерации")
    
    if markets:
        logger.info("✓ Найдено %s рынков для теста", len(markets))
        test_market = markets[0]
        market_id = str(test_market.get("id", ""))
        title = test_market.get("title") or test_market.get("question") or test_market.get("name") or "N/A"
        logger.info("  Тестовый рынок: %s (ID: %s)", title, market_id)
        
        # Показываем структуру первого рынка для отладки
        logger.debug("  Структура рынка (первые ключи): %s", list(test_market.keys())[:10])

        # Получаем orderbook для тестового рынка
        if market_id and market_id != "N/A":
            logger.info("\nПопытка получить orderbook для market_id=%s...", market_id)
            orderbook = client.get_orderbook(market_id)
            if orderbook:
                logger.info("✓ Orderbook получен для market_id=%s", market_id)
                logger.info("  Структура orderbook:")
                if isinstance(orderbook, dict):
                    for key in list(orderbook.keys())[:10]:  # Показываем первые 10 ключей
                        value = orderbook[key]
                        if isinstance(value, (list, dict)):
                            logger.info("    - %s: %s элементов", key, len(value) if hasattr(value, '__len__') else "N/A")
                        else:
                            logger.info("    - %s: %s", key, str(value)[:50])
                else:
                    logger.info("    (не словарь: %s)", type(orderbook))
            else:
                logger.warning("⚠ Не удалось получить orderbook для market_id=%s", market_id)
                logger.info("  Возможные причины:")
                logger.info("    - Рынок не имеет активных ордеров")
                logger.info("    - Неверный формат market_id или token_id")
                logger.info("    - Проблемы с подключением к CLOB API")
        else:
            logger.warning("⚠ Не удалось извлечь market_id из данных рынка")
    else:
        logger.warning("⚠ Рынки не найдены. Возможные причины:")
        logger.warning("  - API возвращает данные в неожиданном формате")
        logger.warning("  - Требуется пагинация для получения всех рынков")
        logger.warning("  - Проблемы с подключением к API")

    # 6. Тест dry-run ордеров
    logger.info("\n[6/6] Тест dry-run режима для ордеров...")
    if markets:
        test_market_id = str(markets[0].get("id", ""))
        logger.info("Тестируем dry-run buy для market_id=%s", test_market_id)
        dry_run_buy = client.buy_outcome(
            market_id=test_market_id,
            outcome=0,
            amount_usdc=10.0,
            max_price=0.5,
            dry_run=True,
        )
        if dry_run_buy:
            logger.info("✓ Dry-run buy успешен:")
            pprint(dry_run_buy)

        logger.info("Тестируем dry-run sell для market_id=%s", test_market_id)
        dry_run_sell = client.sell_outcome(
            market_id=test_market_id,
            outcome=0,
            amount_usdc=10.0,
            min_price=0.5,
            dry_run=True,
        )
        if dry_run_sell:
            logger.info("✓ Dry-run sell успешен:")
            pprint(dry_run_sell)

    logger.info("\n" + "=" * 60)
    logger.info("ТЕСТОВЫЙ ПРОГОН ЗАВЕРШЁН")
    logger.info("=" * 60)


async def run_price_monitor() -> None:
    """
    Запустить мониторинг цен Binance в реальном времени.
    """
    logger.info("=" * 60)
    logger.info("ЗАПУСК МОНИТОРИНГА ЦЕН BINANCE")
    logger.info("=" * 60)
    
    monitor = PriceMonitor()
    
    # Обработка сигналов для graceful shutdown
    def signal_handler(sig, frame):
        logger.info("\nПолучен сигнал остановки, завершаем работу...")
        asyncio.create_task(monitor.stop())
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Запускаем мониторинг
        await monitor.start()
        
        # Ждем бесконечно (мониторинг работает в фоне)
        logger.info("Мониторинг цен запущен. Нажмите Ctrl+C для остановки.")
        while monitor.is_running:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nОстановка мониторинга...")
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка в мониторинге цен: %s", exc)
    finally:
        await monitor.stop()
        logger.info("Мониторинг цен остановлен")


async def run_orchestrator() -> None:
    logger.info("=" * 60)
    logger.info("ЗАПУСК ORCHESTRATOR (SIMULATION MODE)")
    logger.info("=" * 60)

    orchestrator = TradingOrchestrator(app_config)
    stop_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info("\nПолучен сигнал остановки, завершаем работу...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        check_geoblock()
        await orchestrator.start()
        logger.info("Orchestrator запущен. Нажмите Ctrl+C для остановки.")
        while not stop_event.is_set():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("\nОстановка orchestrator...")
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка в orchestrator: %s", exc)
    finally:
        await orchestrator.stop()
        logger.info("Orchestrator остановлен")

def main() -> None:
    """
    Главная функция с выбором режима работы.
    """
    # Проверка аргументов командной строки
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_run()
            return
        elif sys.argv[1] == "monitor" or sys.argv[1] == "price":
            # Запускаем мониторинг цен
            asyncio.run(run_price_monitor())
            return
        elif sys.argv[1] == "orchestrator" or sys.argv[1] == "sim":
            asyncio.run(run_orchestrator())
            return

    logger.info("Инициализация Polymarket клиента...")
    try:
        check_geoblock()
        client = PolymarketClient()
    except Exception as exc:  # noqa: BLE001
        logger.error("Не удалось инициализировать клиента: %s", exc)
        return

    # Обновляем рынки при запуске приложения (если кэш устарел)
    logger.info("Обновление рынков при запуске приложения (если кэш устарел)...")
    client.check_and_update_markets(force=False)
    
    # Запускаем фоновый поток для автоматического обновления каждую минуту
    # (будет проверять, прошло ли 15 минут с последнего обновления)
    client.start_auto_update()
    
    try:
        market_id = choose_market(client)
        if not market_id:
            logger.info("Рынок не выбран, завершаем работу.")
            return

        show_orderbook(client, market_id)

        # По умолчанию НИЧЕГО не покупаем/продаём, пока пользователь явно не подтвердит.
        if confirm("Хотите перейти к созданию ордера (buy/sell)?"):
            interactive_trade(client, market_id)
        else:
            logger.info("Создание ордеров пропущено по выбору пользователя.")
    finally:
        # Останавливаем фоновый поток при выходе
        client.stop_auto_update()


if __name__ == "__main__":
    main()

