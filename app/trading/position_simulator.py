import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class PositionStatus(Enum):
    """Статус позиции."""

    OPEN = "OPEN"
    CLOSED = "CLOSED"
    CLOSED_PROFIT = "CLOSED_PROFIT"
    CLOSED_LOSS = "CLOSED_LOSS"


@dataclass
class SimulatedPosition:
    """Одна открытая позиция в симуляции."""

    position_id: str
    entry_time: datetime
    entry_decision: str  # "ENTER_BOTH", "ENTER_UP", "ENTER_DOWN"
    market_id: Optional[str] = None

    # Для BOTH sides (pair-cost arb)
    up_qty: float = 0.0
    up_avg_price: float = 0.0
    down_qty: float = 0.0
    down_avg_price: float = 0.0

    # Для ONE side (directional)
    side: Optional[str] = None  # "UP" или "DOWN"
    qty: float = 0.0
    avg_price: float = 0.0

    # Tracking
    status: PositionStatus = PositionStatus.OPEN
    exit_time: Optional[datetime] = None
    exit_reason: Optional[str] = None

    # PnL
    entry_cost: float = 0.0
    exit_revenue: float = 0.0
    pnl: float = 0.0
    pnl_pct: float = 0.0

    trading_fees: float = 0.0

    momentum_at_entry: dict = field(default_factory=dict)
    candle_info_at_entry: dict = field(default_factory=dict)


class PositionSimulator:
    """
    Симулирует открытие, отслеживание и закрытие позиций.
    Не совершает реальные ордера, просто отслеживает P&L.
    """

    def __init__(self, initial_balance: float, trading_fee_pct: float = 0.02):
        self.initial_balance = initial_balance
        self.trading_fee_pct = trading_fee_pct

        self.current_balance = initial_balance
        self.positions: dict[str, SimulatedPosition] = {}
        self.closed_positions: list[SimulatedPosition] = []

        self.position_counter = 0

    def open_position(
        self,
        decision: str,
        orderbook,
        entry_conditions,
        position_size_pct: float = 0.1,
        market_id: Optional[str] = None,
    ) -> Optional[SimulatedPosition]:
        position_size_usd = self.current_balance * position_size_pct

        if position_size_usd > self.current_balance:
            logger.warning(
                "Insufficient balance: need $%.2f, have $%.2f",
                position_size_usd,
                self.current_balance,
            )
            return None

        self.position_counter += 1
        position_id = f"SIM_{self.position_counter}_{datetime.now().timestamp()}"

        position = SimulatedPosition(
            position_id=position_id,
            entry_time=datetime.now(),
            entry_decision=decision,
            market_id=market_id,
            momentum_at_entry=getattr(entry_conditions, "momentum_values", {}),
            candle_info_at_entry=getattr(entry_conditions, "candle_analysis", {}),
        )

        if decision == "ENTER_BOTH":
            up_investment = position_size_usd / 2
            down_investment = position_size_usd / 2

            position.up_qty = up_investment / orderbook.ask_up
            position.up_avg_price = orderbook.ask_up

            position.down_qty = down_investment / orderbook.ask_down
            position.down_avg_price = orderbook.ask_down

            position.entry_cost = up_investment + down_investment

            logger.info(
                "[SIM] Position %s OPEN (BOTH): UP: %.2f @ $%.4f (ask size: %.2f) | DOWN: %.2f @ $%.4f (ask size: %.2f) | Cost: $%.2f",
                position_id,
                position.up_qty,
                position.up_avg_price,
                orderbook.ask_up_size,
                position.down_qty,
                position.down_avg_price,
                orderbook.ask_down_size,
                position.entry_cost,
            )

        elif decision == "ENTER_UP":
            position.side = "UP"
            position.qty = position_size_usd / orderbook.ask_up
            position.avg_price = orderbook.ask_up
            position.entry_cost = position_size_usd

            logger.info(
                "[SIM] Position %s OPEN (UP): %.2f @ $%.4f (ask size: %.2f) | Cost: $%.2f",
                position_id,
                position.qty,
                position.avg_price,
                orderbook.ask_up_size,
                position.entry_cost,
            )

        elif decision == "ENTER_DOWN":
            position.side = "DOWN"
            position.qty = position_size_usd / orderbook.ask_down
            position.avg_price = orderbook.ask_down
            position.entry_cost = position_size_usd

            logger.info(
                "[SIM] Position %s OPEN (DOWN): %.2f @ $%.4f (ask size: %.2f) | Cost: $%.2f",
                position_id,
                position.qty,
                position.avg_price,
                orderbook.ask_down_size,
                position.entry_cost,
            )

        else:
            logger.error("Unknown decision type: %s", decision)
            return None

        # Логируем стакан (last1) после входа
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

        raw = getattr(orderbook, "raw_orderbook", None)
        if isinstance(raw, list) and raw:
            # [UP, DOWN]
            up = raw[0] if len(raw) > 0 else {}
            down = raw[1] if len(raw) > 1 else {}
            up_bids = _normalize_side((up or {}).get("bids"))
            up_asks = _normalize_side((up or {}).get("asks"))
            down_bids = _normalize_side((down or {}).get("bids"))
            down_asks = _normalize_side((down or {}).get("asks"))
            logger.info(
                "[SIM] Orderbook UP 🟢 (last1) bid=%s ask=%s",
                up_bids[-1:] if up_bids else [],
                up_asks[-1:] if up_asks else [],
            )
            logger.info(
                "[SIM] Orderbook DOWN 🔴 (last1) bid=%s ask=%s",
                down_bids[-1:] if down_bids else [],
                down_asks[-1:] if down_asks else [],
            )
        elif isinstance(raw, dict):
            bids = _normalize_side(raw.get("bids"))
            asks = _normalize_side(raw.get("asks"))
            logger.info("[SIM] Orderbook (last1) bid=%s ask=%s", bids[-1:], asks[-1:])

        entry_fees = position.entry_cost * self.trading_fee_pct
        position.trading_fees += entry_fees
        self.current_balance -= (position.entry_cost + entry_fees)

        self.positions[position_id] = position

        logger.info("[SIM] Balance after entry: $%.2f", self.current_balance)

        return position

    def add_to_position(
        self,
        position_id: str,
        side: str,
        amount_usd: float,
        ask_price: float,
        ask_size: float = 0.0,
    ) -> bool:
        if position_id not in self.positions:
            logger.warning("Position %s not found for add", position_id)
            return False
        if amount_usd <= 0:
            return False
        if amount_usd > self.current_balance:
            logger.warning(
                "Insufficient balance for add: need $%.2f, have $%.2f",
                amount_usd,
                self.current_balance,
            )
            return False
        if ask_price <= 0:
            return False

        position = self.positions[position_id]
        qty_add = amount_usd / ask_price
        entry_fees = amount_usd * self.trading_fee_pct

        if position.entry_decision == "ENTER_BOTH":
            if side == "UP":
                position.up_avg_price = (
                    (position.up_qty * position.up_avg_price) + (qty_add * ask_price)
                ) / (position.up_qty + qty_add)
                position.up_qty += qty_add
            elif side == "DOWN":
                position.down_avg_price = (
                    (position.down_qty * position.down_avg_price) + (qty_add * ask_price)
                ) / (position.down_qty + qty_add)
                position.down_qty += qty_add
            else:
                logger.warning("Unknown side for add: %s", side)
                return False
        else:
            if position.side != side:
                # Конвертируем в BOTH, чтобы можно было докупить противоположную сторону
                if position.side == "UP":
                    position.up_qty = position.qty
                    position.up_avg_price = position.avg_price
                elif position.side == "DOWN":
                    position.down_qty = position.qty
                    position.down_avg_price = position.avg_price
                position.entry_decision = "ENTER_BOTH"
                position.side = None
                position.qty = 0.0
                position.avg_price = 0.0
                # После конвертации докупаем как для BOTH
                if side == "UP":
                    position.up_avg_price = (
                        (position.up_qty * position.up_avg_price) + (qty_add * ask_price)
                    ) / (position.up_qty + qty_add)
                    position.up_qty += qty_add
                elif side == "DOWN":
                    position.down_avg_price = (
                        (position.down_qty * position.down_avg_price) + (qty_add * ask_price)
                    ) / (position.down_qty + qty_add)
                    position.down_qty += qty_add
                else:
                    logger.warning("Unknown side for add: %s", side)
                    return False
            else:
                position.avg_price = (
                    (position.qty * position.avg_price) + (qty_add * ask_price)
                ) / (position.qty + qty_add)
                position.qty += qty_add

        position.entry_cost += amount_usd
        position.trading_fees += entry_fees
        self.current_balance -= (amount_usd + entry_fees)

        logger.info(
            "[SIM] Position %s ADD (%s): %.2f @ $%.4f (ask size: %.2f) | Cost: $%.2f",
            position_id,
            side,
            qty_add,
            ask_price,
            ask_size,
            amount_usd,
        )
        return True

    def has_traded_market(self, market_id: Optional[str]) -> bool:
        if not market_id:
            return False
        if any(p.market_id == market_id for p in self.positions.values()):
            return True
        if any(p.market_id == market_id for p in self.closed_positions):
            return True
        return False

    def get_open_position_by_market(self, market_id: Optional[str]) -> Optional[SimulatedPosition]:
        if not market_id:
            return None
        for position in self.positions.values():
            if position.market_id == market_id and position.status == PositionStatus.OPEN:
                return position
        return None

    def add_to_position(
        self,
        market_id: str,
        side: str,
        ask_price: float,
        amount_usdc: float,
    ) -> bool:
        position = self.get_open_position_by_market(market_id)
        if not position:
            logger.info("[SIM] No open position to add for market %s", market_id)
            return False
        if amount_usdc <= 0:
            return False
        if amount_usdc > self.current_balance:
            logger.warning(
                "[SIM] Insufficient balance to add: need $%.2f, have $%.2f",
                amount_usdc,
                self.current_balance,
            )
            return False

        side = side.upper()
        qty = amount_usdc / ask_price

        # Конвертируем в BOTH, если нужно
        if position.entry_decision != "ENTER_BOTH" and side in {"UP", "DOWN"}:
            if position.side and position.side != side:
                # Переносим текущую single-side позицию в BOTH
                if position.side == "UP":
                    position.up_qty = position.qty
                    position.up_avg_price = position.avg_price
                elif position.side == "DOWN":
                    position.down_qty = position.qty
                    position.down_avg_price = position.avg_price
                position.side = None
                position.qty = 0.0
                position.avg_price = 0.0
                position.entry_decision = "ENTER_BOTH"

        if position.entry_decision == "ENTER_BOTH":
            if side == "UP":
                total_cost = position.up_qty * position.up_avg_price + amount_usdc
                position.up_qty += qty
                position.up_avg_price = total_cost / position.up_qty if position.up_qty else 0.0
            else:
                total_cost = position.down_qty * position.down_avg_price + amount_usdc
                position.down_qty += qty
                position.down_avg_price = total_cost / position.down_qty if position.down_qty else 0.0
        else:
            # Single-side add
            if not position.side:
                position.side = side
            total_cost = position.qty * position.avg_price + amount_usdc
            position.qty += qty
            position.avg_price = total_cost / position.qty if position.qty else 0.0

        position.entry_cost += amount_usdc
        entry_fees = amount_usdc * self.trading_fee_pct
        position.trading_fees += entry_fees
        self.current_balance -= (amount_usdc + entry_fees)

        logger.info(
            "[SIM] Add to position %s (%s): +$%.2f @ $%.4f | New balance: $%.2f",
            position.position_id,
            side,
            amount_usdc,
            ask_price,
            self.current_balance,
        )
        return True

    def close_position(
        self,
        position_id: str,
        current_bid_up: float,
        current_bid_down: float,
        exit_reason: str,
    ) -> Optional[SimulatedPosition]:
        if position_id not in self.positions:
            logger.error("Position %s not found", position_id)
            return None

        position = self.positions[position_id]

        if position.entry_decision == "ENTER_BOTH":
            exit_revenue_up = position.up_qty * current_bid_up
            exit_revenue_down = position.down_qty * current_bid_down
            position.exit_revenue = exit_revenue_up + exit_revenue_down

            logger.info(
                "[SIM] Position %s CLOSE (BOTH): UP: %.2f @ $%.4f = $%.2f | DOWN: %.2f @ $%.4f = $%.2f | Revenue: $%.2f",
                position_id,
                position.up_qty,
                current_bid_up,
                exit_revenue_up,
                position.down_qty,
                current_bid_down,
                exit_revenue_down,
                position.exit_revenue,
            )

        elif position.side == "UP":
            position.exit_revenue = position.qty * current_bid_up
            logger.info(
                "[SIM] Position %s CLOSE (UP): %.2f @ $%.4f = $%.2f",
                position_id,
                position.qty,
                current_bid_up,
                position.exit_revenue,
            )

        elif position.side == "DOWN":
            position.exit_revenue = position.qty * current_bid_down
            logger.info(
                "[SIM] Position %s CLOSE (DOWN): %.2f @ $%.4f = $%.2f",
                position_id,
                position.qty,
                current_bid_down,
                position.exit_revenue,
            )

        if exit_reason == "market_closed" and position.exit_revenue < position.entry_cost:
            # На закрытии рынка убыточные позиции считаем полностью сгоревшими
            position.exit_revenue = 0.0

        exit_fees = position.exit_revenue * self.trading_fee_pct
        position.trading_fees += exit_fees

        position.pnl = position.exit_revenue - position.entry_cost - position.trading_fees
        position.pnl_pct = (
            (position.pnl / position.entry_cost) * 100.0 if position.entry_cost > 0 else 0.0
        )

        position.exit_time = datetime.now()
        position.exit_reason = exit_reason
        position.status = (
            PositionStatus.CLOSED_PROFIT
            if position.pnl > 0
            else PositionStatus.CLOSED_LOSS
            if position.pnl < 0
            else PositionStatus.CLOSED
        )

        self.current_balance += position.exit_revenue - exit_fees

        pnl_emoji = "✅" if position.pnl > 0 else "❌" if position.pnl < 0 else "⚪"
        logger.warning(
            "%s [SIM] Position %s CLOSED: P&L: $%.2f (%.2f%%) | Reason: %s | Balance: $%.2f",
            pnl_emoji,
            position_id,
            position.pnl,
            position.pnl_pct,
            exit_reason,
            self.current_balance,
        )

        del self.positions[position_id]
        self.closed_positions.append(position)

        return position

    def get_stats(self) -> dict:
        total_closed = len(self.closed_positions)
        winning_positions = sum(1 for p in self.closed_positions if p.pnl > 0)
        losing_positions = sum(1 for p in self.closed_positions if p.pnl < 0)

        total_pnl = sum(p.pnl for p in self.closed_positions)
        total_fees = sum(p.trading_fees for p in self.closed_positions)

        win_rate = (winning_positions / total_closed * 100) if total_closed > 0 else 0.0

        return {
            "initial_balance": self.initial_balance,
            "current_balance": self.current_balance,
            "total_pnl": total_pnl,
            "pnl_pct": (total_pnl / self.initial_balance) * 100,
            "total_fees": total_fees,
            "open_positions": len(self.positions),
            "closed_positions": total_closed,
            "winning_positions": winning_positions,
            "losing_positions": losing_positions,
            "win_rate": win_rate,
        }

    def print_summary(self) -> None:
        stats = self.get_stats()
        logger.info(
            "[SIM SUMMARY] Balance: $%.2f | P&L: $%.2f (%.2f%%) | Closed: %s (W: %s, L: %s, WR: %.1f%%) | Open: %s",
            stats["current_balance"],
            stats["total_pnl"],
            stats["pnl_pct"],
            stats["closed_positions"],
            stats["winning_positions"],
            stats["losing_positions"],
            stats["win_rate"],
            stats["open_positions"],
        )
