import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from app.config import (
    BUY_MORE_PROFITABLE_FIRST_ENABLE,
    BUY_MORE_PROFITABLE_FIRST_MAX_ASK,
    MOMENTUM_RULE_ENABLE,
)
logger = logging.getLogger(__name__)


class EntryDecision(Enum):
    SKIP = "SKIP"
    ENTER_BOTH = "ENTER_BOTH"
    ENTER_UP = "ENTER_UP"
    ENTER_DOWN = "ENTER_DOWN"


@dataclass
class EntryConditions:
    decision: EntryDecision
    reason: str
    momentum_values: dict
    candle_analysis: dict


class SignalRouter:
    """Placeholder Signal Router. Возвращает SKIP, пока не добавлена логика."""

    def __init__(self, price_monitor, polymarket_connector, config):
        self.price_monitor = price_monitor
        self.polymarket_connector = polymarket_connector
        self.config = config

    async def process_signal(self, symbol: str) -> Optional[EntryConditions]:
        stats = self.price_monitor.get_stats(symbol)
        if not stats:
            return EntryConditions(
                decision=EntryDecision.SKIP,
                reason="No stats available",
                momentum_values={},
                candle_analysis={},
            )

        momentums = stats.get("momentums", {}) or {}

        if MOMENTUM_RULE_ENABLE:
            signal = self.price_monitor.get_current_signal(symbol)
            if not signal:
                return EntryConditions(
                    decision=EntryDecision.SKIP,
                    reason="No signal",
                    momentum_values=momentums,
                    candle_analysis=stats.get("candles", {}) or {},
                )
            direction = signal.get("direction")
            if direction == "UP":
                decision = EntryDecision.ENTER_UP
            elif direction == "DOWN":
                decision = EntryDecision.ENTER_DOWN
            else:
                decision = EntryDecision.SKIP

            return EntryConditions(
                decision=decision,
                reason=f"Signal {direction}",
                momentum_values=momentums,
                candle_analysis=stats.get("candles", {}) or {},
            )

        # MOMENTUM_RULE_ENABLE = False -> используем только другие правила входа
        market_id = self.polymarket_connector.market_id
        if self.polymarket_connector.has_traded_market(market_id):
            return EntryConditions(
                decision=EntryDecision.SKIP,
                reason="Entry rules apply only to first trade",
                momentum_values=momentums,
                candle_analysis=stats.get("candles", {}) or {},
            )
        if not BUY_MORE_PROFITABLE_FIRST_ENABLE:
            return EntryConditions(
                decision=EntryDecision.SKIP,
                reason="No entry rules enabled",
                momentum_values=momentums,
                candle_analysis=stats.get("candles", {}) or {},
            )

        orderbook = await self.polymarket_connector.get_orderbook_snapshot()
        if not orderbook:
            return EntryConditions(
                decision=EntryDecision.SKIP,
                reason="No orderbook for entry rule",
                momentum_values=momentums,
                candle_analysis=stats.get("candles", {}) or {},
            )

        up_ok = orderbook.ask_up < BUY_MORE_PROFITABLE_FIRST_MAX_ASK
        down_ok = orderbook.ask_down < BUY_MORE_PROFITABLE_FIRST_MAX_ASK

        if up_ok and down_ok:
            decision = EntryDecision.ENTER_BOTH
            reason = (
                f"BUY_MORE_PROFITABLE_FIRST: ask_up={orderbook.ask_up:.4f} "
                f"ask_down={orderbook.ask_down:.4f} < {BUY_MORE_PROFITABLE_FIRST_MAX_ASK:.4f}"
            )
            logger.info("✅ Entry rule triggered: BUY_MORE_PROFITABLE_FIRST (UP+DOWN)")
        elif up_ok:
            decision = EntryDecision.ENTER_UP
            reason = (
                f"BUY_MORE_PROFITABLE_FIRST: ask_up={orderbook.ask_up:.4f} "
                f"< {BUY_MORE_PROFITABLE_FIRST_MAX_ASK:.4f}"
            )
            logger.info("✅ Entry rule triggered: BUY_MORE_PROFITABLE_FIRST (UP)")
        elif down_ok:
            decision = EntryDecision.ENTER_DOWN
            reason = (
                f"BUY_MORE_PROFITABLE_FIRST: ask_down={orderbook.ask_down:.4f} "
                f"< {BUY_MORE_PROFITABLE_FIRST_MAX_ASK:.4f}"
            )
            logger.info("✅ Entry rule triggered: BUY_MORE_PROFITABLE_FIRST (DOWN)")
        else:
            decision = EntryDecision.SKIP
            reason = (
                f"BUY_MORE_PROFITABLE_FIRST: ask_up={orderbook.ask_up:.4f} "
                f"ask_down={orderbook.ask_down:.4f} >= {BUY_MORE_PROFITABLE_FIRST_MAX_ASK:.4f}"
            )

        return EntryConditions(
            decision=decision,
            reason=reason,
            momentum_values=momentums,
            candle_analysis=stats.get("candles", {}) or {},
        )
