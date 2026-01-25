import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Optional

import websockets
from websockets import InvalidStatusCode, InvalidURI, WebSocketException

from app.filter_generator import get_polymarket_time
from app.polymarket_client import PolymarketClient

logger = logging.getLogger(__name__)


@dataclass
class OrderbookSnapshot:
    bid_up: float
    bid_down: float
    ask_up: float
    ask_down: float
    bid_up_size: float = 0.0
    bid_down_size: float = 0.0
    ask_up_size: float = 0.0
    ask_down_size: float = 0.0
    raw_orderbook: object | None = None


class PolymarketConnector:
    """Минимальный connector для получения orderbook snapshot."""

    def __init__(self, config, market_id: Optional[str] = None) -> None:
        self.config = config
        self.market_id = market_id
        self.client: Optional[PolymarketClient] = None
        self._last_snapshot: Optional[OrderbookSnapshot] = None
        self._last_snapshot_at: float = 0.0
        self._orderbook_cache_seconds: float = float(
            getattr(config, "ORDERBOOK_CACHE_SECONDS", 1.0)
        )
        self._orderbook_stale_seconds: float = float(
            getattr(config, "ORDERBOOK_STALE_SECONDS", 10.0)
        )
        self._ws_enabled: bool = bool(getattr(config, "polymarket_ws_enabled", False))
        self._ws_url: str = str(
            getattr(config, "polymarket_ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws")
        )
        self._ws_urls: list[str] = list(
            getattr(config, "polymarket_ws_urls", [self._ws_url]) or [self._ws_url]
        )
        self._ws_ping_interval: float = float(
            getattr(config, "polymarket_ws_ping_interval", 20)
        )
        self._ws_ping_timeout: float = float(
            getattr(config, "polymarket_ws_ping_timeout", 20)
        )
        self._ws_close_timeout: float = float(
            getattr(config, "polymarket_ws_close_timeout", 10)
        )
        self._ws_compression = getattr(config, "polymarket_ws_compression", None)
        self._ws_reconnect_seconds: float = float(
            getattr(config, "polymarket_ws_reconnect_seconds", 2)
        )
        self._ws_stale_seconds: float = float(
            getattr(config, "polymarket_ws_stale_seconds", 5)
        )
        self._ws_subscribe_type: str = str(
            getattr(config, "polymarket_ws_subscribe_type", "market")
        )
        self._ws_subscribe_assets_key: str = str(
            getattr(config, "polymarket_ws_subscribe_assets_key", "assets_ids")
        )
        self._ws_origin: Optional[str] = getattr(config, "polymarket_ws_origin", None)
        self._ws_headers: dict[str, str] = dict(
            getattr(config, "polymarket_ws_headers", {}) or {}
        )
        self._debug_enabled: bool = bool(
            getattr(config, "polymarket_orderbook_debug_enabled", False)
        )
        self._debug_interval: float = float(
            getattr(config, "polymarket_orderbook_debug_interval_seconds", 5)
        )
        self._debug_log_interval: float = float(
            getattr(config, "orderbook_debug_log_interval_seconds", self._debug_interval)
        )
        self._enable_spread: bool = bool(getattr(config, "enable_spread", True))
        self._spread_log_interval: float = float(
            getattr(config, "spread_log_interval_seconds", self._debug_interval)
        )
        self._debug_task: Optional[asyncio.Task] = None
        self._price_getter: Optional[Callable[[str], Optional[float]]] = None
        self._current_market_title: Optional[str] = None
        self._current_symbol: Optional[str] = None
        self._market_start_time = None
        self._market_end_time = None
        self._price_to_beat: Optional[float] = None
        self._price_to_beat_at = None
        self._price_to_beat_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop_event = asyncio.Event()
        self._ws_orderbooks: dict[str, dict] = {}
        self._ws_orderbook_at: dict[str, float] = {}
        self._ws_token_ids: list[str] = []
        self._ws_outcome_map: dict[str, str] = {}
        self._traded_markets: list[str] = []
        self._last_debug_log_at: float = 0.0
        self._last_debug_log_at_by_label: dict[str, float] = {}
        self._last_spread_log_at: float = 0.0

    async def initialize(self) -> None:
        if not self.client:
            self.client = PolymarketClient()
            # Обновляем кэш рынков при старте оркестратора
            self.client.check_and_update_markets(force=False)
            await self.refresh_market_id()
            await self._restart_orderbook_ws()

    async def stop(self) -> None:
        await self._stop_orderbook_ws()
        await self._stop_orderbook_debug()
        await self._stop_price_to_beat_task()
        if self.client:
            self.client.log_last_update_info(prefix="Market cache (shutdown)")

    async def refresh_market_id(self) -> None:
        """Перефильтровать рынки из кэша и выбрать актуальный."""
        if not self.client:
            return
        markets = self.client.get_markets(use_cache=True)
        if not markets:
            self.market_id = None
            logger.info("Нет рынков после фильтрации, пропускаем сделки.")
            return
        first_market = markets[0]
        market_id = str(first_market.get("id", ""))
        if market_id:
            market_changed = market_id != self.market_id
            self.market_id = market_id
            title = (
                first_market.get("title")
                or first_market.get("question")
                or first_market.get("name")
                or first_market.get("slug", "N/A")
            )
            if market_changed or title != self._current_market_title:
                self._current_market_title = title or None
                self._current_symbol = self._infer_symbol_from_title(title)
                await self._schedule_price_to_beat_capture()
                if self.client and self._current_market_title:
                    self._market_end_time = self.client.get_market_end_time(self._current_market_title)
            logger.info(
                "Выбран market_id для оркестратора: %s [id=%s]",
                title,
                self.market_id,
            )
            await self._restart_orderbook_ws()
            await self._restart_orderbook_debug()
            await self._log_market_metrics(title)
            # Тестовый orderbook для отладки (каждый цикл обновления рынка)
            await self._log_orderbook_by_tokens_once()
            if self._enable_spread:
                snapshot = await self.get_orderbook_snapshot()
                self.log_spread(snapshot, context="after market selection")

    def _log_orderbook_debug(self, snapshot: Optional[OrderbookSnapshot]) -> None:
        if not snapshot:
            logger.info("Orderbook debug: пустой")
            return
        if self.has_traded_market(self.market_id):
            return
        now = time.monotonic()
        if self._debug_log_interval > 0 and now - self._last_debug_log_at < self._debug_log_interval:
            return
        self._last_debug_log_at = now
        raw = snapshot.raw_orderbook

        def _normalize_side(side):
            if not side:
                return []
            out = []
            for item in side:
                if isinstance(item, dict):
                    out.append((item.get("price"), item.get("size")))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append((item[0], item[1]))
            return out

        if isinstance(raw, list) and raw:
            up = raw[0] if len(raw) > 0 else {}
            down = raw[1] if len(raw) > 1 else {}
            up_bids = _normalize_side((up or {}).get("bids"))
            up_asks = _normalize_side((up or {}).get("asks"))
            down_bids = _normalize_side((down or {}).get("bids"))
            down_asks = _normalize_side((down or {}).get("asks"))
            logger.info(
                "Orderbook debug UP (last1) bid=%s ask=%s",
                up_bids[-1:] if up_bids else [],
                up_asks[-1:] if up_asks else [],
            )
            logger.info(
                "Orderbook debug DOWN (last1) bid=%s ask=%s",
                down_bids[-1:] if down_bids else [],
                down_asks[-1:] if down_asks else [],
            )
        elif isinstance(raw, dict):
            bids = _normalize_side(raw.get("bids"))
            asks = _normalize_side(raw.get("asks"))
            logger.info("Orderbook debug (last1) bid=%s ask=%s", bids[-1:], asks[-1:])

    def set_market_id(self, market_id: str) -> None:
        self.market_id = market_id

    def set_price_getter(self, price_getter: Callable[[str], Optional[float]]) -> None:
        self._price_getter = price_getter

    def get_current_symbol(self) -> Optional[str]:
        return self._current_symbol

    def get_price_to_beat(self) -> Optional[float]:
        return self._price_to_beat

    def get_minutes_to_market_close(self) -> Optional[float]:
        if not self._market_end_time:
            return None
        now = get_polymarket_time()
        return (self._market_end_time - now).total_seconds() / 60

    def get_minutes_since_market_open(self) -> Optional[float]:
        if not self._market_start_time:
            return None
        now = get_polymarket_time()
        return (now - self._market_start_time).total_seconds() / 60

    def mark_market_traded(self, market_id: str) -> None:
        if not market_id:
            return
        if market_id in self._traded_markets:
            return
        self._traded_markets.append(market_id)

    def has_traded_market(self, market_id: str | None) -> bool:
        if not market_id:
            return False
        return market_id in self._traded_markets

    def log_spread(self, snapshot: Optional[OrderbookSnapshot], context: str = "") -> None:
        if not self._enable_spread or not snapshot:
            return
        now = time.monotonic()
        if self._spread_log_interval > 0 and now - self._last_spread_log_at < self._spread_log_interval:
            return
        self._last_spread_log_at = now
        def _normalize_side(side):
            if not side:
                return []
            out = []
            for item in side:
                if isinstance(item, dict):
                    out.append((item.get("price"), item.get("size")))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append((item[0], item[1]))
            return out

        bid_up = snapshot.bid_up
        ask_up = snapshot.ask_up
        bid_down = snapshot.bid_down
        ask_down = snapshot.ask_down

        raw = getattr(snapshot, "raw_orderbook", None)
        if isinstance(raw, list) and raw:
            up = raw[0] if len(raw) > 0 else {}
            down = raw[1] if len(raw) > 1 else {}
            up_bids = _normalize_side((up or {}).get("bids"))
            up_asks = _normalize_side((up or {}).get("asks"))
            down_bids = _normalize_side((down or {}).get("bids"))
            down_asks = _normalize_side((down or {}).get("asks"))
            if up_bids and up_asks:
                up_bids_last1 = up_bids[-1:]
                up_asks_last1 = up_asks[-1:]
                try:
                    bid_up = float(up_bids_last1[0][0])
                    ask_up = float(up_asks_last1[0][0])
                except Exception:
                    pass
            if down_bids and down_asks:
                down_bids_last1 = down_bids[-1:]
                down_asks_last1 = down_asks[-1:]
                try:
                    bid_down = float(down_bids_last1[0][0])
                    ask_down = float(down_asks_last1[0][0])
                except Exception:
                    pass

        try:
            spread_up = (float(ask_up) - float(bid_up)) * 100
            spread_down = (float(ask_down) - float(bid_down)) * 100
        except Exception:
            return
        context_suffix = f" ({context})" if context else ""
        logger.info(
            "Spread%s: UP %.2f (ask=%.4f bid=%.4f) | DOWN %.2f (ask=%.4f bid=%.4f)",
            context_suffix,
            spread_up,
            ask_up,
            bid_up,
            spread_down,
            ask_down,
            bid_down,
        )

    def log_orderbook_snapshot(self, snapshot: Optional[OrderbookSnapshot], context: str = "") -> None:
        if not snapshot:
            return
        now = time.monotonic()
        if self._debug_log_interval > 0 and now - self._last_debug_log_at < self._debug_log_interval:
            return
        self._last_debug_log_at = now
        raw = snapshot.raw_orderbook

        def _normalize_side(side):
            if not side:
                return []
            out = []
            for item in side:
                if isinstance(item, dict):
                    out.append((item.get("price"), item.get("size")))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append((item[0], item[1]))
            return out

        if isinstance(raw, list) and raw:
            up = raw[0] if len(raw) > 0 else {}
            down = raw[1] if len(raw) > 1 else {}
            up_bids = _normalize_side((up or {}).get("bids"))
            up_asks = _normalize_side((up or {}).get("asks"))
            down_bids = _normalize_side((down or {}).get("bids"))
            down_asks = _normalize_side((down or {}).get("asks"))
            suffix = f" ({context})" if context else ""
            logger.info(
                "Orderbook UP 🟢%s (last1) bid=%s ask=%s",
                suffix,
                up_bids[-1:] if up_bids else [],
                up_asks[-1:] if up_asks else [],
            )
            logger.info(
                "Orderbook DOWN 🔴%s (last1) bid=%s ask=%s",
                suffix,
                down_bids[-1:] if down_bids else [],
                down_asks[-1:] if down_asks else [],
            )

    async def _restart_orderbook_debug(self) -> None:
        await self._stop_orderbook_debug()
        if not self._debug_enabled or not self.client or not self.market_id:
            return
        self._debug_task = asyncio.create_task(self._orderbook_debug_loop())

    async def _stop_orderbook_debug(self) -> None:
        if self._debug_task:
            self._debug_task.cancel()
            try:
                await self._debug_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass
            self._debug_task = None

    async def _orderbook_debug_loop(self) -> None:
        while True:
            try:
                if not self.client or not self.market_id:
                    await asyncio.sleep(self._debug_interval)
                    continue

                token_ids, outcomes = self.client.get_market_token_ids(self.market_id)
                if not token_ids:
                    await asyncio.sleep(self._debug_interval)
                    continue

                outcome_map = {}
                if outcomes and len(outcomes) == len(token_ids):
                    outcome_map = {
                        str(token_id): str(outcome).lower()
                        for token_id, outcome in zip(token_ids, outcomes)
                    }

                await self._log_orderbook_by_tokens(token_ids, outcome_map)

                await asyncio.sleep(self._debug_interval)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.debug("Orderbook debug loop error: %s", exc)
                await asyncio.sleep(self._debug_interval)

    async def _log_orderbook_by_tokens_once(self) -> None:
        if not self._debug_enabled or not self.client or not self.market_id:
            return
        token_ids, outcomes = self.client.get_market_token_ids(self.market_id)
        if not token_ids:
            snapshot = await self.get_orderbook_snapshot()
            self._log_orderbook_debug(snapshot)
            return
        outcome_map = {}
        if outcomes and len(outcomes) == len(token_ids):
            outcome_map = {
                str(token_id): str(outcome).lower()
                for token_id, outcome in zip(token_ids, outcomes)
            }
        await self._log_orderbook_by_tokens(token_ids, outcome_map)

    async def _log_market_metrics(self, title: str) -> None:
        if not self.client or not self.market_id:
            return
        market_data = self.client.get_market_data(self.market_id)
        if not market_data:
            return
        line = market_data.get("line")
        last_trade = market_data.get("lastTradePrice") or market_data.get("lastTradePriceNum")
        best_bid = market_data.get("bestBid") or market_data.get("bestBidNum")
        best_ask = market_data.get("bestAsk") or market_data.get("bestAskNum")
        logger.info(
            "Market data: line=%s lastTradePrice=%s bestBid=%s bestAsk=%s",
            line,
            last_trade,
            best_bid,
            best_ask,
        )

        if self._price_getter is None or line is None:
            return
        symbol = self._infer_symbol_from_title(title)
        if not symbol:
            return
        current_price = self._price_getter(symbol)
        try:
            line_val = float(line)
            price_val = float(current_price) if current_price is not None else None
        except Exception:
            return
        if price_val is None:
            return
        deviation = price_val - line_val
        logger.info(
            "Market deviation: symbol=%s line=%.2f price=%.2f deviation=%.2f",
            symbol,
            line_val,
            price_val,
            deviation,
        )

    def _infer_symbol_from_title(self, title: str) -> Optional[str]:
        if not title:
            return None
        lowered = title.lower()
        if "bitcoin" in lowered:
            return "BTCUSDT"
        if "ethereum" in lowered:
            return "ETHUSDT"
        if "solana" in lowered:
            return "SOLUSDT"
        return None

    async def _schedule_price_to_beat_capture(self) -> None:
        await self._stop_price_to_beat_task()
        self._price_to_beat = None
        self._price_to_beat_at = None
        self._market_start_time = None

        if not self.client or not self._current_market_title:
            return

        self._market_start_time = self.client.get_market_start_time(self._current_market_title)
        if not self._market_start_time:
            logger.info("Не удалось определить время старта рынка для price_to_beat.")
            return

        if not self._price_getter or not self._current_symbol:
            logger.info("Нет источника цены или символа для price_to_beat.")
            return

        self._price_to_beat_task = asyncio.create_task(self._price_to_beat_loop())

    async def _stop_price_to_beat_task(self) -> None:
        if self._price_to_beat_task:
            self._price_to_beat_task.cancel()
            try:
                await self._price_to_beat_task
            except asyncio.CancelledError:
                pass
            self._price_to_beat_task = None

    async def _price_to_beat_loop(self) -> None:
        if not self._market_start_time or not self._price_getter or not self._current_symbol:
            return
        while True:
            now = get_polymarket_time()
            if now >= self._market_start_time:
                price = self._price_getter(self._current_symbol)
                if price is not None:
                    self._price_to_beat = price
                    self._price_to_beat_at = now
                    logger.info(
                        "Captured price_to_beat: symbol=%s price=%.2f start=%s",
                        self._current_symbol,
                        price,
                        self._market_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    return
            await asyncio.sleep(1)

    async def _log_orderbook_by_tokens(
        self,
        token_ids: list[str],
        outcome_map: dict[str, str],
    ) -> None:
        for idx, token_id in enumerate(token_ids):
            book = await asyncio.to_thread(
                self.client.get_orderbook_by_token_id,
                token_id,
            )
            if not isinstance(book, dict):
                continue
            outcome = outcome_map.get(str(token_id))
            if not outcome:
                outcome = "up" if idx == 0 else "down"
            if outcome in {"up", "yes"}:
                label = "UP 🟢"
            elif outcome in {"down", "no"}:
                label = "DOWN 🔴"
            else:
                label = f"{outcome} ({token_id})"
            self._log_orderbook_by_label(book, label)

    def _log_orderbook_by_label(self, orderbook: dict, label: str) -> None:
        if self.has_traded_market(self.market_id):
            return
        now = time.monotonic()
        last_at = self._last_debug_log_at_by_label.get(label, 0.0)
        if self._debug_log_interval > 0 and now - last_at < self._debug_log_interval:
            return
        self._last_debug_log_at_by_label[label] = now

        def _normalize_side(side):
            if not side:
                return []
            out = []
            for item in side:
                if isinstance(item, dict):
                    out.append((item.get("price"), item.get("size")))
                elif isinstance(item, (list, tuple)) and len(item) >= 2:
                    out.append((item[0], item[1]))
            return out

        bids = _normalize_side(orderbook.get("bids") or [])
        asks = _normalize_side(orderbook.get("asks") or [])
        logger.info(
            "Orderbook debug %s (last1) bid=%s ask=%s",
            label,
            bids[-1:],
            asks[-1:],
        )

    async def _restart_orderbook_ws(self) -> None:
        await self._stop_orderbook_ws()
        if not self._ws_enabled or not self.client or not self.market_id:
            return

        token_ids, outcomes = self.client.get_market_token_ids(self.market_id)
        if not token_ids:
            logger.warning("WS orderbook: token_ids не найдены для market_id=%s", self.market_id)

        self._ws_token_ids = token_ids
        if outcomes and len(outcomes) == len(token_ids):
            self._ws_outcome_map = {
                str(token_id): str(outcome).lower()
                for token_id, outcome in zip(token_ids, outcomes)
            }
        else:
            self._ws_outcome_map = {}

        self._ws_stop_event.clear()
        self._ws_task = asyncio.create_task(self._orderbook_ws_loop(token_ids))

    async def _stop_orderbook_ws(self) -> None:
        if self._ws_task:
            self._ws_stop_event.set()
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                pass
            self._ws_task = None
        self._ws_orderbooks.clear()
        self._ws_orderbook_at.clear()
        self._ws_token_ids = []
        self._ws_outcome_map = {}

    async def _orderbook_ws_loop(self, token_ids: list[str]) -> None:
        while not self._ws_stop_event.is_set():
            try:
                connected = False
                for ws_url in self._ws_urls:
                    try:
                        async with websockets.connect(
                            ws_url,
                            ping_interval=self._ws_ping_interval,
                            ping_timeout=self._ws_ping_timeout,
                            close_timeout=self._ws_close_timeout,
                            compression=self._ws_compression,
                            origin=self._ws_origin,
                            extra_headers=self._ws_headers or None,
                        ) as ws:
                            logger.info("WS orderbook: подключились к %s", ws_url)
                            connected = True
                            await self._ws_subscribe(ws, token_ids)
                            async for raw in ws:
                                if self._ws_stop_event.is_set():
                                    break
                                try:
                                    message = json.loads(raw)
                                except Exception:
                                    continue
                                self._handle_ws_message(message)
                        if connected:
                            break
                    except InvalidStatusCode as exc:
                        logger.warning(
                            "WS orderbook: статус=%s при подключении (url=%s)",
                            exc.status_code,
                            ws_url,
                        )
                        continue
                if connected:
                    continue
                await asyncio.sleep(self._ws_reconnect_seconds)
            except asyncio.CancelledError:
                break
            except InvalidURI as exc:
                logger.warning("WS orderbook: неверный URI %s (%s)", self._ws_url, exc)
                await asyncio.sleep(self._ws_reconnect_seconds)
            except WebSocketException as exc:
                logger.warning("WS orderbook: websocket error: %r", exc)
                await asyncio.sleep(self._ws_reconnect_seconds)
            except ConnectionResetError as exc:
                logger.warning(
                    "WS orderbook: соединение сброшено сервером: %r",
                    exc,
                )
                await asyncio.sleep(self._ws_reconnect_seconds)
            except Exception as exc:  # noqa: BLE001
                logger.warning("WS orderbook: ошибка соединения: %r", exc)
                await asyncio.sleep(self._ws_reconnect_seconds)

    async def _ws_subscribe(self, ws, token_ids: list[str]) -> None:
        if self._ws_subscribe_type == "market" and self.market_id:
            payload = {
                "type": self._ws_subscribe_type,
                self._ws_subscribe_assets_key: [self.market_id],
            }
            await ws.send(json.dumps(payload))
            logger.info("WS orderbook: подписка по market_id=%s", self.market_id)
            return
        logger.warning("WS orderbook: неподдерживаемый тип подписки: %s", self._ws_subscribe_type)

    def _handle_ws_message(self, message: dict) -> None:
        if not isinstance(message, dict):
            return

        if message.get("event_type") == "book" and "bids" in message and "asks" in message:
            market_id = message.get("market_id") or self.market_id
            if market_id == self.market_id:
                self._ws_orderbooks["__market__"] = {
                    "bids": message.get("bids") or [],
                    "asks": message.get("asks") or [],
                }
                self._ws_orderbook_at["__market__"] = time.time()
            return

        payload = message.get("data") if isinstance(message, dict) else None
        if payload is None and isinstance(message, dict):
            payload = message

        if not isinstance(payload, dict):
            return

        token_id = (
            payload.get("token_id")
            or payload.get("tokenId")
            or payload.get("asset_id")
            or payload.get("assetId")
        )
        if not token_id and len(self._ws_token_ids) == 1:
            token_id = self._ws_token_ids[0]

        orderbook = None
        if "bids" in payload and "asks" in payload:
            orderbook = {"bids": payload.get("bids") or [], "asks": payload.get("asks") or []}
        elif "book" in payload and isinstance(payload.get("book"), dict):
            orderbook = payload.get("book")
        elif "orderbook" in payload and isinstance(payload.get("orderbook"), dict):
            orderbook = payload.get("orderbook")

        if token_id and isinstance(orderbook, dict):
            self._ws_orderbooks[str(token_id)] = orderbook
            self._ws_orderbook_at[str(token_id)] = time.time()

    async def get_orderbook_snapshot(self) -> Optional[OrderbookSnapshot]:
        if not self.client or not self.market_id:
            logger.warning("Orderbook snapshot недоступен: client или market_id не установлен.")
            return None

        try:
            now = time.time()
            if (
                self._last_snapshot
                and now - self._last_snapshot_at < self._orderbook_cache_seconds
            ):
                return self._last_snapshot

            token_ids, outcomes = self.client.get_market_token_ids(self.market_id)
            if len(token_ids) >= 2:
                outcome_map = {}
                if outcomes and len(outcomes) == len(token_ids):
                    outcome_map = {
                        str(token_id): str(outcome).lower()
                        for token_id, outcome in zip(token_ids, outcomes)
                    }

                books = await asyncio.gather(
                    *[
                        asyncio.to_thread(self.client.get_orderbook_by_token_id, token_id)
                        for token_id in token_ids[:2]
                    ]
                )
                token_to_book = {
                    token_id: book
                    for token_id, book in zip(token_ids[:2], books)
                    if isinstance(book, dict)
                }

                up_token = None
                down_token = None
                for token_id, outcome in outcome_map.items():
                    if outcome in {"up", "yes"}:
                        up_token = token_id
                    elif outcome in {"down", "no"}:
                        down_token = token_id

                if not up_token or not down_token:
                    up_token = token_ids[0]
                    down_token = token_ids[1]

                up_ob = token_to_book.get(up_token)
                down_ob = token_to_book.get(down_token)
                if isinstance(up_ob, dict) and isinstance(down_ob, dict):
                    snapshot = self._build_snapshot_from_orderbooks(up_ob, down_ob)
                    if snapshot:
                        self._last_snapshot = snapshot
                        self._last_snapshot_at = time.time()
                        return snapshot

            ws_snapshot = self._build_snapshot_from_ws()
            if ws_snapshot:
                self._last_snapshot = ws_snapshot
                self._last_snapshot_at = time.time()
                return ws_snapshot

            orderbook = self.client.get_orderbook(self.market_id)

            # Если пришел список из двух orderbook'ов (UP/DOWN)
            if isinstance(orderbook, list) and len(orderbook) >= 2:
                snapshot = self._build_snapshot_from_orderbooks(orderbook[0], orderbook[1])
                if snapshot:
                    self._last_snapshot = snapshot
                    self._last_snapshot_at = time.time()
                    return snapshot

            if isinstance(orderbook, dict):
                bids = orderbook.get("bids") or []
                asks = orderbook.get("asks") or []
                bid_up = self._best_bid_price(bids)
                ask_up = self._best_ask_price(asks)
                if ask_up <= 0:
                    logger.warning(
                        "Orderbook пустой или без ask (market_id=%s).",
                        self.market_id,
                    )
                    logger.info(
                        "RAW orderbook (market_id=%s): %s",
                        self.market_id,
                        orderbook,
                    )
                    return None
                # Без явного разделения UP/DOWN — используем те же цены.
                snapshot = OrderbookSnapshot(
                    bid_up=bid_up,
                    bid_down=bid_up,
                    ask_up=ask_up,
                    ask_down=ask_up,
                    bid_up_size=self._best_bid_size(bids),
                    ask_up_size=self._best_ask_size(asks),
                    bid_down_size=self._best_bid_size(bids),
                    ask_down_size=self._best_ask_size(asks),
                    raw_orderbook=orderbook,
                )
                self._last_snapshot = snapshot
                self._last_snapshot_at = time.time()
                return snapshot

            logger.warning("Неверный формат orderbook.")
            if self._last_snapshot and (time.time() - self._last_snapshot_at) < self._orderbook_stale_seconds:
                logger.warning(
                    "Orderbook формат неверный, используем кэш (age=%.1fs).",
                    time.time() - self._last_snapshot_at,
                )
                return self._last_snapshot
            return None
        except Exception as exc:  # noqa: BLE001
            if self._last_snapshot and (time.time() - self._last_snapshot_at) < self._orderbook_stale_seconds:
                logger.warning(
                    "Не удалось распарсить orderbook: %s. Используем кэш (age=%.1fs).",
                    exc,
                    time.time() - self._last_snapshot_at,
                )
                return self._last_snapshot
            logger.warning("Не удалось распарсить orderbook: %s", exc)
            return None

    def _price_from_entry(self, entry):
        if isinstance(entry, dict):
            return float(entry.get("price", 0.0))
        if isinstance(entry, (list, tuple)) and entry:
            return float(entry[0])
        return 0.0

    def _size_from_entry(self, entry):
        if isinstance(entry, dict):
            return float(entry.get("size", 0.0))
        if isinstance(entry, (list, tuple)) and len(entry) > 1:
            return float(entry[1])
        return 0.0

    def _best_bid_price(self, side):
        return self._price_from_entry(side[0]) if side else 0.0

    def _best_bid_size(self, side):
        return self._size_from_entry(side[0]) if side else 0.0

    def _best_ask_price(self, side):
        return self._price_from_entry(side[-1]) if side else 0.0

    def _best_ask_size(self, side):
        return self._size_from_entry(side[-1]) if side else 0.0

    def _build_snapshot_from_orderbooks(self, up_ob: dict, down_ob: dict) -> Optional[OrderbookSnapshot]:
        bids_up = (up_ob or {}).get("bids") or []
        asks_up = (up_ob or {}).get("asks") or []
        bids_down = (down_ob or {}).get("bids") or []
        asks_down = (down_ob or {}).get("asks") or []
        return OrderbookSnapshot(
            bid_up=self._best_bid_price(bids_up),
            ask_up=self._best_ask_price(asks_up),
            bid_down=self._best_bid_price(bids_down),
            ask_down=self._best_ask_price(asks_down),
            bid_up_size=self._best_bid_size(bids_up),
            ask_up_size=self._best_ask_size(asks_up),
            bid_down_size=self._best_bid_size(bids_down),
            ask_down_size=self._best_ask_size(asks_down),
            raw_orderbook=[up_ob, down_ob],
        )
    def _build_snapshot_from_ws(self) -> Optional[OrderbookSnapshot]:
        if not self._ws_enabled:
            return None

        def _is_fresh(token_id: str) -> bool:
            ts = self._ws_orderbook_at.get(token_id, 0.0)
            return (time.time() - ts) <= self._ws_stale_seconds

        orderbooks = {
            tid: ob
            for tid, ob in self._ws_orderbooks.items()
            if tid in self._ws_token_ids and _is_fresh(tid)
        }
        market_book = self._ws_orderbooks.get("__market__")
        market_book_ts = self._ws_orderbook_at.get("__market__", 0.0)
        if (
            isinstance(market_book, dict)
            and (time.time() - market_book_ts) <= self._ws_stale_seconds
            and ("bids" in market_book and "asks" in market_book)
        ):
            bids = market_book.get("bids") or []
            asks = market_book.get("asks") or []
            return OrderbookSnapshot(
                bid_up=best_bid_price(bids),
                ask_up=best_ask_price(asks),
                bid_down=best_bid_price(bids),
                ask_down=best_ask_price(asks),
                bid_up_size=best_bid_size(bids),
                ask_up_size=best_ask_size(asks),
                bid_down_size=best_bid_size(bids),
                ask_down_size=best_ask_size(asks),
                raw_orderbook=market_book,
            )
        if len(orderbooks) < 2:
            return None

        def _price_from_entry(entry):
            if isinstance(entry, dict):
                return float(entry.get("price", 0.0))
            if isinstance(entry, (list, tuple)) and entry:
                return float(entry[0])
            return 0.0

        def _size_from_entry(entry):
            if isinstance(entry, dict):
                return float(entry.get("size", 0.0))
            if isinstance(entry, (list, tuple)) and len(entry) > 1:
                return float(entry[1])
            return 0.0

        def best_bid_price(side):
            return _price_from_entry(side[0]) if side else 0.0

        def best_bid_size(side):
            return _size_from_entry(side[0]) if side else 0.0

        def best_ask_price(side):
            return _price_from_entry(side[-1]) if side else 0.0

        def best_ask_size(side):
            return _size_from_entry(side[-1]) if side else 0.0

        up_token = None
        down_token = None
        for token_id, outcome in self._ws_outcome_map.items():
            if outcome == "up":
                up_token = token_id
            elif outcome == "down":
                down_token = token_id

        if not up_token or not down_token:
            if len(self._ws_token_ids) >= 2:
                up_token = self._ws_token_ids[0]
                down_token = self._ws_token_ids[1]

        if not up_token or not down_token:
            return None

        up_ob = orderbooks.get(up_token)
        down_ob = orderbooks.get(down_token)
        if not isinstance(up_ob, dict) or not isinstance(down_ob, dict):
            return None

        bids_up = up_ob.get("bids") or []
        asks_up = up_ob.get("asks") or []
        bids_down = down_ob.get("bids") or []
        asks_down = down_ob.get("asks") or []

        return OrderbookSnapshot(
            bid_up=best_bid_price(bids_up),
            ask_up=best_ask_price(asks_up),
            bid_down=best_bid_price(bids_down),
            ask_down=best_ask_price(asks_down),
            bid_up_size=best_bid_size(bids_up),
            ask_up_size=best_ask_size(asks_up),
            bid_down_size=best_bid_size(bids_down),
            ask_down_size=best_ask_size(asks_down),
            raw_orderbook=[up_ob, down_ob],
        )
