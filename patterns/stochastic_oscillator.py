# stochastic_oscillator.py
from typing import List, Dict, Tuple, Optional

def calculate_stochastic_oscillator(
    candles: List[Dict[str, float]], 
    k_period: int = 14, 
    d_period: int = 3
) -> Tuple[Optional[float], Optional[float]]:
    """
    Рассчитывает Стохастический осциллятор (%K и %D).
    """
    needed = k_period + d_period - 1
    if len(candles) < needed:
        return None, None

    relevant_candles = candles[-needed:]
    highs = [float(candle['high']) for candle in relevant_candles]
    lows = [float(candle['low']) for candle in relevant_candles]
    closes = [float(candle['close']) for candle in relevant_candles]

    percent_k_list = []
    for i in range(k_period - 1, len(closes)):
        highest_high = max(highs[i - k_period + 1:i + 1])
        lowest_low = min(lows[i - k_period + 1:i + 1])
        current_close = closes[i]

        if highest_high == lowest_low:
            percent_k = 0
        else:
            percent_k = ((current_close - lowest_low) / (highest_high - lowest_low)) * 100

        percent_k_list.append(percent_k)

    if len(percent_k_list) < d_period:
        return None, None

    percent_d_list = []
    for i in range(d_period - 1, len(percent_k_list)):
        percent_d = sum(percent_k_list[i - d_period + 1:i + 1]) / d_period
        percent_d_list.append(percent_d)

    last_percent_k = percent_k_list[-1]
    last_percent_d = percent_d_list[-1]

    return last_percent_k, last_percent_d

def analyze_stochastic(
    candles: List[Dict[str, float]], 
    k_period: int = 14, 
    d_period: int = 3, 
    oversold: float = 20, 
    overbought: float = 80
) -> Dict[str, Optional[float]]:
    """
    Анализирует стохастический осциллятор для заданных свечей.
    
    Оптимизирует количество свечей для расчёта, используя только необходимые (k_period + d_period - 1).
    Рассчитывает %K и %D и генерирует сигнал:
      - 'stochastic_long', если оба значения ниже уровня перепроданности (oversold),
      - 'stochastic_short', если оба значения выше уровня перекупленности (overbought),
      - None, если условия не выполнены.
    """
    needed = k_period + d_period - 1
    if len(candles) > needed:
        candles = candles[-needed:]
        
    percent_k, percent_d = calculate_stochastic_oscillator(candles, k_period, d_period)
    signal = None
    if percent_k is not None and percent_d is not None:
        if percent_k < oversold and percent_d < oversold:
            signal = 'stochastic_long'
        elif percent_k > overbought and percent_d > overbought:
            signal = 'stochastic_short'
    return {'%K': percent_k, '%D': percent_d, 'signal': signal}
