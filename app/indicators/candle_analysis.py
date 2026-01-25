from dataclasses import dataclass
from typing import Optional
import logging

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class CandleStreak:
    """Информация о streak (подряд идущих свечах)."""

    color: str
    count: int
    start_idx: int
    strength: float


@dataclass
class CandleAnalysis:
    """Результат анализа свечей."""

    last_candle_color: str
    current_streak: CandleStreak
    total_green_count: int
    total_red_count: int
    green_percentage: float


class CandleAnalyzer:
    """Анализирует цвет свечей и streaks."""

    @staticmethod
    def get_candle_color(row: pd.Series) -> str:
        """
        Определяет цвет свечи.

        Args:
            row: pd.Series с 'open', 'close'
        """
        return "green" if row["close"] > row["open"] else "red"

    @staticmethod
    def analyze_df(df: pd.DataFrame, min_candles: int = 10) -> Optional[CandleAnalysis]:
        """
        Анализирует DataFrame свечей и возвращает информацию о streaks.
        """
        if df is None or df.empty or len(df) < min_candles:
            return None

        df_copy = df.copy()
        if "open" not in df_copy.columns or "close" not in df_copy.columns:
            logger.warning("Недостаточно колонок для анализа свечей.")
            return None

        df_copy["color"] = df_copy.apply(CandleAnalyzer.get_candle_color, axis=1)

        total_green = int((df_copy["color"] == "green").sum())
        total_red = int((df_copy["color"] == "red").sum())
        total = len(df_copy)
        green_percentage = (total_green / total * 100) if total > 0 else 0.0

        last_candle_color = str(df_copy["color"].iloc[-1])

        colors = df_copy["color"].values
        last_color = colors[-1]

        streak_count = 1
        for i in range(len(colors) - 2, -1, -1):
            if colors[i] == last_color:
                streak_count += 1
            else:
                break

        start_idx = len(colors) - streak_count
        streak_df = df_copy.iloc[start_idx:]

        strength_total = 0.0
        for _, row in streak_df.iterrows():
            if row["open"] == 0:
                continue
            move = (row["close"] - row["open"]) / row["open"] * 100.0
            strength_total += abs(move)
        strength = strength_total / streak_count if streak_count > 0 else 0.0

        current_streak = CandleStreak(
            color=str(last_color),
            count=streak_count,
            start_idx=start_idx,
            strength=strength,
        )

        return CandleAnalysis(
            last_candle_color=last_candle_color,
            current_streak=current_streak,
            total_green_count=total_green,
            total_red_count=total_red,
            green_percentage=green_percentage,
        )

    @staticmethod
    def format_candle_log(
        analysis: CandleAnalysis,
        timeframe: str,
        candle_colors_emoji: dict,
    ) -> str:
        """Форматирует результаты анализа для логирования."""
        green_emoji = candle_colors_emoji.get("green", "🟢")
        red_emoji = candle_colors_emoji.get("red", "🔴")

        last_color_emoji = (
            green_emoji if analysis.last_candle_color == "green" else red_emoji
        )
        streak_color_emoji = (
            green_emoji if analysis.current_streak.color == "green" else red_emoji
        )

        return (
            f"{timeframe}: {last_color_emoji} | "
            f"Streak: {streak_color_emoji}x{analysis.current_streak.count} "
            f"(strength: {analysis.current_streak.strength:.2f}%) | "
            f"Green: {analysis.total_green_count}/"
            f"{analysis.total_green_count + analysis.total_red_count} "
            f"({analysis.green_percentage:.1f}%)"
        )
