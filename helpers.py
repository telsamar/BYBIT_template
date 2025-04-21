# helpers.py

import os
import logging
from typing import List, Dict, Optional

from patterns.stochastic_oscillator import calculate_stochastic_oscillator
from patterns.macd import calculate_macd

logger = logging.getLogger(__name__)

def analyze_candles(
    candles: List[Dict[str, float]],
    k_period: int = int(os.getenv("K_PERIOD", 14)),
    d_period: int = int(os.getenv("D_PERIOD", 3)),
    macd_fast: int = int(os.getenv("FAST_PERIOD", 12)),
    macd_slow: int = int(os.getenv("SLOW_PERIOD", 26)),
    macd_signal: int = int(os.getenv("SIGNAL_PERIOD", 9)),
    overbought: float = float(os.getenv("OVERBOUGHT", 90.0)),
    oversold: float = float(os.getenv("OVERSOLD", 10.0)),
) -> Dict[str, Optional[float]]:
    """
    Анализирует свечи на основе стохастического осциллятора и MACD.

    Генерация сигнала:
      - 'long', если %K < oversold (по стохастику) и MACD > 0
      - 'short', если %K > overbought и MACD < 0
      - None в остальных случаях

    """
    analysis: Dict[str, Optional[float]] = {}

    # Стохастик
    percent_k, percent_d = calculate_stochastic_oscillator(candles, k_period, d_period)
    analysis["%K"] = percent_k
    analysis["%D"] = percent_d

    # MACD
    macd_line, signal_line, histogram = calculate_macd(
        candles, fast_period=macd_fast, slow_period=macd_slow, signal_period=macd_signal
    )
    # берём последнее значение MACD‑линии
    last_macd = macd_line[-1] if isinstance(macd_line, list) else macd_line
    analysis["MACD"] = last_macd

    # Определяем сигнал
    signal: Optional[str] = None
    if percent_k is not None and last_macd is not None:
        if percent_k < oversold and last_macd > 0:
            signal = "long"
        elif percent_k > overbought and last_macd < 0:
            signal = "short"

    analysis["signal"] = signal

    logger.info("Анализ завершён. Результат: %r", analysis)
    return analysis
