# macd.py
from typing import List, Dict, Tuple, Optional

def calculate_macd(candles: List[Dict[str, float]], fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Рассчитывает MACD, сигнальную линию и гистограмму.
    """
    if len(candles) < slow_period + signal_period:
        return None, None, None

    closes = [float(candle['close']) for candle in candles]

    def calculate_ema(data: List[float], period: int) -> List[Optional[float]]:
        ema = []
        multiplier = 2 / (period + 1)
        for i in range(len(data)):
            if i < period - 1:
                ema.append(None)
            elif i == period - 1:
                sma = sum(data[:period]) / period
                ema.append(sma)
            else:
                ema_value = (data[i] - ema[-1]) * multiplier + ema[-1]
                ema.append(ema_value)
        return ema

    ema_fast = calculate_ema(closes, fast_period)
    ema_slow = calculate_ema(closes, slow_period)

    macd_line = []
    for fast, slow in zip(ema_fast, ema_slow):
        if fast is None or slow is None:
            macd_line.append(None)
        else:
            macd_line.append(fast - slow)

    valid_macd = [m for m in macd_line if m is not None]

    if len(valid_macd) < signal_period:
        return None, None, None

    ema_signal = calculate_ema(valid_macd, signal_period)

    histogram = []
    for m, s in zip(valid_macd, ema_signal):
        if m is None or s is None:
            histogram.append(None)
        else:
            histogram.append(m - s)

    last_macd = valid_macd[-1] if valid_macd else None
    last_signal = ema_signal[-1] if ema_signal else None
    last_histogram = histogram[-1] if histogram else None

    return last_macd, last_signal, last_histogram
