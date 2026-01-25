import asyncio
import logging
import time
from datetime import datetime

from app.config import (
    POST_ENTRY_ADD_USD,
    POST_ENTRY_CHANGE_PCT_MAX,
    POST_ENTRY_CHANGE_PCT_ADD_SAME_MIN,
    POST_ENTRY_EXIT_BID_TARGET,
    POST_ENTRY_EXIT_CHANGE_PCT_MIN,
    POST_ENTRY_MAX_ASK,
    POST_ENTRY_MINUTES_TO_CLOSE_MAX,
    POST_ENTRY_MINUTES_TO_CLOSE_MIN,
    POST_ENTRY_RULES_ENABLE,
    FIRST_ENTRY_MINUTES_TO_CLOSE_MIN,
    FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN,
    SIMULATION_INITIAL_BALANCE,
    SIMULATION_MODE,
    SIMULATION_POSITION_SIZE_PCT,
    SIMULATION_TRADING_FEE_PCT,
    SIGNAL_ENTRY_CHECK_SECONDS,
    PRICE_TO_BEAT_LOG_SECONDS,
)
from app.connectors.polymarket import PolymarketConnector
from app.connectors.price_monitor import PriceMonitor
from app.trading.position_simulator import PositionSimulator
from app.trading.signal_router import EntryDecision, SignalRouter

logger = logging.getLogger(__name__)


class TradingOrchestrator:
    """
    Главный оркестратор торговли с поддержкой режима симуляции.
    """

    def __init__(self, config):
        self.config = config
        self.simulation_mode = SIMULATION_MODE
        self.price_monitor: PriceMonitor | None = None
        self.polymarket_connector: PolymarketConnector | None = None
        self.signal_router: SignalRouter | None = None
        self.position_simulator: PositionSimulator | None = None

        self.entry_history: list[dict] = []
        self.market_entry_records: list[dict] = []
        self.last_decision = None
        self._price_deviation_task: asyncio.Task | None = None
        self._spread_task: asyncio.Task | None = None
        self._spread_interval_seconds: float = float(
            getattr(config, "polymarket_orderbook_debug_interval_seconds", 5)
        )
        self._enable_spread: bool = bool(getattr(config, "enable_spread", True))
        self._post_entry_log_interval_seconds: float = float(
            getattr(config, "post_entry_position_log_interval_seconds", 10)
        )
        self._post_entry_action_log_interval_seconds: float = float(
            getattr(config, "post_entry_action_log_interval_seconds", 10)
        )
        self._post_entry_skip_logs_enabled: bool = bool(
            getattr(config, "post_entry_skip_logs_enabled", True)
        )
        self._post_entry_rule_check_logs_enabled: bool = bool(
            getattr(config, "post_entry_rule_check_logs_enabled", True)
        )
        self._post_entry_rule_check_log_interval_seconds: float = float(
            getattr(config, "post_entry_rule_check_log_interval_seconds", 10)
        )
        self._post_entry_log_last_at: float = 0.0
        self._post_entry_action_log_last_at: float = 0.0
        self._post_entry_check_log_last_at: float = 0.0

    async def start(self) -> None:
        logger.info(
            "Starting Trading Orchestrator... (SIMULATION_MODE=%s)",
            self.simulation_mode,
        )

        self.price_monitor = PriceMonitor()
        self.polymarket_connector = PolymarketConnector(self.config)
        self.polymarket_connector.set_price_getter(self.price_monitor.get_price)
        self.signal_router = SignalRouter(
            self.price_monitor,
            self.polymarket_connector,
            self.config,
        )

        if self.simulation_mode:
            self.position_simulator = PositionSimulator(
                initial_balance=SIMULATION_INITIAL_BALANCE,
                trading_fee_pct=SIMULATION_TRADING_FEE_PCT,
            )
            logger.info("[SIM] Initialized with balance: $%.2f", SIMULATION_INITIAL_BALANCE)

        await self.polymarket_connector.initialize()

        asyncio.create_task(self.price_monitor.start())
        await asyncio.sleep(1)

        asyncio.create_task(self._signal_router_loop())
        asyncio.create_task(self._market_refresh_loop())

        if self.simulation_mode:
            asyncio.create_task(self._stats_reporter_loop())

        logger.info("Trading Orchestrator started successfully")

    async def stop(self) -> None:
        """Остановить все компоненты."""
        if self.price_monitor:
            await self.price_monitor.stop()
        if self.polymarket_connector:
            await self.polymarket_connector.stop()
        await self._stop_price_deviation_task()
        await self._stop_spread_task()

    async def _signal_router_loop(self) -> None:
        while True:
            try:
                entry_conditions = await self.signal_router.process_signal("BTCUSDT")

                if entry_conditions and entry_conditions.decision != EntryDecision.SKIP:
                    await self._handle_entry_decision(entry_conditions)

                if self.simulation_mode and self.position_simulator:
                    await self._manage_open_positions()

                await asyncio.sleep(SIGNAL_ENTRY_CHECK_SECONDS)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in signal router loop: %s", exc, exc_info=True)
                await asyncio.sleep(1)

    async def _handle_entry_decision(self, entry_conditions) -> None:
        decision = entry_conditions.decision.value
        if self.simulation_mode:
            market_id = self.polymarket_connector.market_id
            if not market_id:
                logger.info("[SIM] Нет актуального рынка, пропускаем сделку.")
                return
            if self.position_simulator.has_traded_market(market_id):
                # Не спамим логами после первой сделки в рынке
                return
            if FIRST_ENTRY_MINUTES_TO_CLOSE_MIN > 0:
                minutes_to_close = self.polymarket_connector.get_minutes_to_market_close()
                if minutes_to_close is None or minutes_to_close <= FIRST_ENTRY_MINUTES_TO_CLOSE_MIN:
                    logger.info(
                        "[SIM] Skip entry: minutes_to_close=%.2f <= %.2f",
                        minutes_to_close if minutes_to_close is not None else -1,
                        FIRST_ENTRY_MINUTES_TO_CLOSE_MIN,
                    )
                    return
            if FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN > 0:
                minutes_since_open = self.polymarket_connector.get_minutes_since_market_open()
                if minutes_since_open is None or minutes_since_open > FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN:
                    logger.info(
                        "[SIM] Skip entry: minutes_since_open=%.2f > %.2f",
                        minutes_since_open if minutes_since_open is not None else -1,
                        FIRST_ENTRY_MAX_MINUTES_AFTER_OPEN,
                    )
                    return
            logger.warning("🚀 ENTRY DECISION: %s | %s", decision, entry_conditions.reason)
            orderbook = await self.polymarket_connector.get_orderbook_snapshot()
            if not orderbook:
                logger.error("Failed to get orderbook for entry")
                return

            if self._enable_spread:
                self.polymarket_connector.log_spread(orderbook, context="pre-entry")

            position = self.position_simulator.open_position(
                decision=decision,
                orderbook=orderbook,
                entry_conditions=entry_conditions,
                position_size_pct=SIMULATION_POSITION_SIZE_PCT,
                market_id=market_id,
            )
            if position:
                entry_record = {
                    "timestamp": datetime.now(),
                    "market_id": market_id,
                    "decision": decision,
                    "entry_cost": position.entry_cost,
                    "ask_up": orderbook.ask_up,
                    "ask_down": orderbook.ask_down,
                }
                self.market_entry_records.append(entry_record)
                self.entry_history.append(
                    {
                        "timestamp": datetime.now(),
                        "decision": decision,
                        "position_id": position.position_id,
                        "entry_conditions": entry_conditions,
                    }
                )
                self.polymarket_connector.mark_market_traded(market_id)
                self._ensure_price_deviation_task()
                self._ensure_spread_task()
        else:
            logger.warning("🚀 ENTRY DECISION: %s | %s", decision, entry_conditions.reason)
            logger.info("Would execute real order (SIMULATION_MODE=False)")

    async def _manage_open_positions(self) -> None:
        if not self.position_simulator or not self.position_simulator.positions:
            return

        orderbook = await self.polymarket_connector.get_orderbook_snapshot()
        if not orderbook:
            return

        positions_to_close: list[dict] = []
        pair_cost_target = getattr(self.config, "PAIR_COST_TARGET", None)
        max_drawdown_pct = getattr(self.config, "MAX_DRAWDOWN_PCT", None)

        for position_id, position in list(self.position_simulator.positions.items()):
            if position.entry_decision == "ENTER_BOTH" and pair_cost_target is not None:
                pair_cost_at_bid = orderbook.bid_up + orderbook.bid_down
                if pair_cost_at_bid <= pair_cost_target:
                    positions_to_close.append(
                        {
                            "position_id": position_id,
                            "reason": "profit_target",
                            "bid_up": orderbook.bid_up,
                            "bid_down": orderbook.bid_down,
                        }
                    )
                    continue

            if max_drawdown_pct is not None:
                if position.entry_decision == "ENTER_BOTH":
                    current_value = (
                        position.up_qty * orderbook.bid_up
                        + position.down_qty * orderbook.bid_down
                    )
                else:
                    if position.side == "UP":
                        current_value = position.qty * orderbook.bid_up
                    else:
                        current_value = position.qty * orderbook.bid_down

                current_pnl = current_value - position.entry_cost
                drawdown_pct = (abs(current_pnl) / position.entry_cost) * 100

                if drawdown_pct > max_drawdown_pct and current_pnl < 0:
                    positions_to_close.append(
                        {
                            "position_id": position_id,
                            "reason": f"hard_stop_loss (drawdown: {drawdown_pct:.1f}%)",
                            "bid_up": orderbook.bid_up,
                            "bid_down": orderbook.bid_down,
                        }
                    )

        for close_info in positions_to_close:
            self.position_simulator.close_position(
                position_id=close_info["position_id"],
                current_bid_up=close_info["bid_up"],
                current_bid_down=close_info["bid_down"],
                exit_reason=close_info["reason"],
            )

    async def _stats_reporter_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(30)
                if self.position_simulator:
                    self.position_simulator.print_summary()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in stats reporter: %s", exc)

    async def _market_refresh_loop(self) -> None:
        """Каждую минуту обновляет выбранный рынок из кэша по времени."""
        while True:
            try:
                await asyncio.sleep(60)
                if self.polymarket_connector and self.position_simulator:
                    if self.position_simulator.positions:
                        continue
                    await self.polymarket_connector.refresh_market_id()
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in market refresh: %s", exc)

    def _ensure_price_deviation_task(self) -> None:
        if self._price_deviation_task and not self._price_deviation_task.done():
            return
        self._price_deviation_task = asyncio.create_task(self._price_deviation_loop())

    async def _stop_price_deviation_task(self) -> None:
        if self._price_deviation_task:
            self._price_deviation_task.cancel()
            try:
                await self._price_deviation_task
            except asyncio.CancelledError:
                pass
            self._price_deviation_task = None

    async def _price_deviation_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(PRICE_TO_BEAT_LOG_SECONDS)
                if not self.polymarket_connector or not self.price_monitor:
                    continue
                market_id = self.polymarket_connector.market_id
                if not market_id:
                    continue
                if (
                    self.simulation_mode
                    and self.position_simulator
                    and not self.position_simulator.has_traded_market(market_id)
                ):
                    continue
                price_to_beat = self.polymarket_connector.get_price_to_beat()
                if price_to_beat is None:
                    continue
                symbol = self.polymarket_connector.get_current_symbol()
                if not symbol:
                    continue
                current_price = self.price_monitor.get_price(symbol)
                if current_price is None:
                    continue
                deviation = current_price - price_to_beat
                deviation_pct = (deviation / price_to_beat) * 100 if price_to_beat else 0.0
                logger.info(
                    "Price deviation: symbol=%s price=%.2f price_to_beat=%.2f deviation=%.2f (%.3f%%)",
                    symbol,
                    current_price,
                    price_to_beat,
                    deviation,
                    deviation_pct,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in price deviation loop: %s", exc, exc_info=True)
                await asyncio.sleep(1)

    def _ensure_spread_task(self) -> None:
        if self._spread_task and not self._spread_task.done():
            return
        if not self._enable_spread:
            return
        self._spread_task = asyncio.create_task(self._spread_loop())

    async def _stop_spread_task(self) -> None:
        if self._spread_task:
            self._spread_task.cancel()
            try:
                await self._spread_task
            except asyncio.CancelledError:
                pass
            self._spread_task = None

    async def _spread_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self._spread_interval_seconds)
                if not self.polymarket_connector:
                    continue
                market_id = self.polymarket_connector.market_id
                if not market_id:
                    continue
                if (
                    self.simulation_mode
                    and self.position_simulator
                    and not self.position_simulator.has_traded_market(market_id)
                ):
                    continue
                orderbook = await self.polymarket_connector.get_orderbook_snapshot()
                if not orderbook:
                    continue
                self.polymarket_connector.log_spread(orderbook, context="post-entry")
                await self._post_entry_rules(orderbook)
            except Exception as exc:  # noqa: BLE001
                logger.error("Error in spread loop: %s", exc, exc_info=True)
                await asyncio.sleep(1)

    async def _post_entry_rules(self, orderbook) -> None:
        if not POST_ENTRY_RULES_ENABLE:
            return
        if not self.position_simulator or not self.position_simulator.positions:
            return
        if not self.polymarket_connector or not self.price_monitor:
            return
        market_id = self.polymarket_connector.market_id
        if not market_id:
            return

        self.polymarket_connector.log_orderbook_snapshot(orderbook, context="post-entry")

        position = None
        for p in self.position_simulator.positions.values():
            if p.market_id == market_id:
                position = p
                break
        if not position:
            return

        symbol = self.polymarket_connector.get_current_symbol()
        current_price = self.price_monitor.get_price(symbol) if symbol else None
        price_to_beat = self.polymarket_connector.get_price_to_beat()
        minutes_to_close = self.polymarket_connector.get_minutes_to_market_close()

        if minutes_to_close is not None and minutes_to_close <= 0:
            # Рынок закрылся -> закрываем позицию по текущим bid и выбираем новый рынок
            self.position_simulator.close_position(
                position_id=position.position_id,
                current_bid_up=orderbook.bid_up,
                current_bid_down=orderbook.bid_down,
                exit_reason="market_closed",
            )
            logger.info(
                "[SIM] Market closed: position=%s closed at bid_up=%.4f bid_down=%.4f",
                position.position_id,
                orderbook.bid_up,
                orderbook.bid_down,
            )
            await self.polymarket_connector.refresh_market_id()
            return

        change_pct = None
        in_right_direction = None
        if current_price is not None and price_to_beat is not None and price_to_beat > 0:
            change_pct = ((current_price - price_to_beat) / price_to_beat) * 100
            moving_up = current_price > price_to_beat
            in_right_direction = (
                moving_up if position.side == "UP"
                else (not moving_up if position.side == "DOWN" else None)
            )

        abs_change = abs(change_pct) if change_pct is not None else None

        if self._post_entry_rule_check_logs_enabled:
            self._log_post_entry_check(
                "[SIM] Post-entry CHECK: side=%s dir=%s change=%.3f%% | asks(up/down)=%.4f/%.4f "
                "| mins_to_close=%.2f | N=%.3f L=%.3f X=%.4f M1=%.2f M2=%.2f P=%.4f J=%.3f",
                position.side,
                "right" if in_right_direction else "wrong" if in_right_direction is not None else "unknown",
                (change_pct or 0.0),
                orderbook.ask_up,
                orderbook.ask_down,
                minutes_to_close if minutes_to_close is not None else -1,
                POST_ENTRY_CHANGE_PCT_MAX,
                POST_ENTRY_CHANGE_PCT_ADD_SAME_MIN,
                POST_ENTRY_MAX_ASK,
                POST_ENTRY_MINUTES_TO_CLOSE_MIN,
                POST_ENTRY_MINUTES_TO_CLOSE_MAX,
                POST_ENTRY_EXIT_BID_TARGET,
                POST_ENTRY_EXIT_CHANGE_PCT_MIN,
            )
        now = time.monotonic()
        if (
            self._post_entry_log_interval_seconds > 0
            and now - self._post_entry_log_last_at >= self._post_entry_log_interval_seconds
        ):
            self._post_entry_log_last_at = now
            if in_right_direction is None:
                direction_emoji = "⚪"
                side_label = position.side or "BOTH"
            else:
                direction_emoji = "🟢" if in_right_direction else "🔴"
                side_label = position.side or "N/A"
            change_val = change_pct or 0.0
            price_val = current_price or 0.0
            beat_val = price_to_beat or 0.0

            if side_label == "UP":
                buy_price = position.avg_price
                buy_vol = position.entry_cost
                logger.info(
                    "[SIM] Position %s %s side=%s change=%.3f%% price=%.2f beat=%.2f | Buy Price=%.4f Vol=$%.2f",
                    position.position_id,
                    direction_emoji,
                    side_label,
                    change_val,
                    price_val,
                    beat_val,
                    buy_price,
                    buy_vol,
                )
            elif side_label == "DOWN":
                buy_price = position.avg_price
                buy_vol = position.entry_cost
                logger.info(
                    "[SIM] Position %s %s side=%s change=%.3f%% price=%.2f beat=%.2f | Buy Price=%.4f Vol=$%.2f",
                    position.position_id,
                    direction_emoji,
                    side_label,
                    change_val,
                    price_val,
                    beat_val,
                    buy_price,
                    buy_vol,
                )
            else:
                logger.info(
                    "[SIM] Position %s %s side=UP change=%.3f%% price=%.2f beat=%.2f | Buy Price=%.4f Vol=$%.2f",
                    position.position_id,
                    direction_emoji,
                    change_val,
                    price_val,
                    beat_val,
                    position.up_avg_price,
                    position.up_qty * position.up_avg_price,
                )
                logger.info(
                    "[SIM] Position %s %s side=DOWN change=%.3f%% price=%.2f beat=%.2f | Buy Price=%.4f Vol=$%.2f",
                    position.position_id,
                    direction_emoji,
                    change_val,
                    price_val,
                    beat_val,
                    position.down_avg_price,
                    position.down_qty * position.down_avg_price,
                )

        if not position.side or change_pct is None:
            return
        if in_right_direction:
            if abs_change < POST_ENTRY_CHANGE_PCT_MAX:
                # Докупаем противоположную сторону (если цена выгодная)
                if minutes_to_close is None or minutes_to_close <= POST_ENTRY_MINUTES_TO_CLOSE_MIN:
                    if self._post_entry_skip_logs_enabled:
                        self._log_post_entry_action(
                            "[SIM] Post-entry BUY_MORE (other side) skip: minutes_to_close=%.2f <= %.2f",
                            minutes_to_close if minutes_to_close is not None else -1,
                            POST_ENTRY_MINUTES_TO_CLOSE_MIN,
                        )
                    return
                other_side = "DOWN" if position.side == "UP" else "UP"
                if other_side == "UP" and orderbook.ask_up <= POST_ENTRY_MAX_ASK:
                    self.position_simulator.add_to_position(
                        position.position_id,
                        "UP",
                        POST_ENTRY_ADD_USD,
                        orderbook.ask_up,
                        orderbook.ask_up_size,
                    )
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (other side): side=UP ask=%.4f change=%.3f%%",
                        orderbook.ask_up,
                        change_pct,
                    )
                elif other_side == "UP" and self._post_entry_skip_logs_enabled:
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (other side) skip: ask_up=%.4f >= %.4f",
                        orderbook.ask_up,
                        POST_ENTRY_MAX_ASK,
                    )
                if other_side == "DOWN" and orderbook.ask_down <= POST_ENTRY_MAX_ASK:
                    self.position_simulator.add_to_position(
                        position.position_id,
                        "DOWN",
                        POST_ENTRY_ADD_USD,
                        orderbook.ask_down,
                        orderbook.ask_down_size,
                    )
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (other side): side=DOWN ask=%.4f change=%.3f%%",
                        orderbook.ask_down,
                        change_pct,
                    )
                elif other_side == "DOWN" and self._post_entry_skip_logs_enabled:
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (other side) skip: ask_down=%.4f >= %.4f",
                        orderbook.ask_down,
                        POST_ENTRY_MAX_ASK,
                    )
            elif abs_change > POST_ENTRY_CHANGE_PCT_ADD_SAME_MIN:
                # Докупаем по стороне позиции (если цена выгодная)
                if minutes_to_close is None or minutes_to_close >= POST_ENTRY_MINUTES_TO_CLOSE_MAX:
                    if self._post_entry_skip_logs_enabled:
                        self._log_post_entry_action(
                            "[SIM] Post-entry BUY_MORE (same side) skip: minutes_to_close=%.2f >= %.2f",
                            minutes_to_close if minutes_to_close is not None else -1,
                            POST_ENTRY_MINUTES_TO_CLOSE_MAX,
                        )
                    return
                if position.side == "UP" and orderbook.ask_up <= POST_ENTRY_MAX_ASK:
                    self.position_simulator.add_to_position(
                        position.position_id,
                        "UP",
                        POST_ENTRY_ADD_USD,
                        orderbook.ask_up,
                        orderbook.ask_up_size,
                    )
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (same side): side=UP ask=%.4f change=%.3f%%",
                        orderbook.ask_up,
                        change_pct,
                    )
                elif position.side == "UP" and self._post_entry_skip_logs_enabled:
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (same side) skip: ask_up=%.4f >= %.4f",
                        orderbook.ask_up,
                        POST_ENTRY_MAX_ASK,
                    )
                if position.side == "DOWN" and orderbook.ask_down <= POST_ENTRY_MAX_ASK:
                    self.position_simulator.add_to_position(
                        position.position_id,
                        "DOWN",
                        POST_ENTRY_ADD_USD,
                        orderbook.ask_down,
                        orderbook.ask_down_size,
                    )
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (same side): side=DOWN ask=%.4f change=%.3f%%",
                        orderbook.ask_down,
                        change_pct,
                    )
                elif position.side == "DOWN" and self._post_entry_skip_logs_enabled:
                    self._log_post_entry_action(
                        "[SIM] Post-entry BUY_MORE (same side) skip: ask_down=%.4f >= %.4f",
                        orderbook.ask_down,
                        POST_ENTRY_MAX_ASK,
                    )
            return
        else:
            if self._post_entry_skip_logs_enabled:
                self._log_post_entry_action(
                    "[SIM] Post-entry BUY_MORE skip: direction=wrong",
                )

        # Движение не в нашу сторону -> пробуем продать по лучшему bid
        sell_price = orderbook.bid_up if position.side == "UP" else orderbook.bid_down
        if (
            sell_price >= POST_ENTRY_EXIT_BID_TARGET
            and abs_change > POST_ENTRY_EXIT_CHANGE_PCT_MIN
        ):
            self.position_simulator.close_position(
                position_id=position.position_id,
                current_bid_up=orderbook.bid_up,
                current_bid_down=orderbook.bid_down,
                exit_reason="post_entry_exit",
            )
            self._log_post_entry_action(
                "[SIM] Post-entry EXIT: side=%s bid=%.4f change=%.3f%%",
                position.side,
                sell_price,
                change_pct,
            )
        else:
            if self._post_entry_skip_logs_enabled:
                self._log_post_entry_action(
                    "[SIM] Post-entry EXIT skip: bid=%.4f < %.4f or |change|=%.3f <= %.3f",
                    sell_price,
                    POST_ENTRY_EXIT_BID_TARGET,
                    abs_change,
                    POST_ENTRY_EXIT_CHANGE_PCT_MIN,
                )

    def _log_post_entry_action(self, message: str, *args) -> None:
        if self._post_entry_action_log_interval_seconds <= 0:
            return
        now = time.monotonic()
        if now - self._post_entry_action_log_last_at < self._post_entry_action_log_interval_seconds:
            return
        self._post_entry_action_log_last_at = now
        logger.info(message, *args)

    def _log_post_entry_check(self, message: str, *args) -> None:
        if self._post_entry_rule_check_log_interval_seconds <= 0:
            return
        now = time.monotonic()
        if now - self._post_entry_check_log_last_at < self._post_entry_rule_check_log_interval_seconds:
            return
        self._post_entry_check_log_last_at = now
        logger.info(message, *args)

