# engulfing.py
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

def check_engulfing_pattern(candles: List[Dict[str, float]]) -> Optional[str]:
    """
    Проверяет паттерн поглощения (бычье или медвежье) на основе последних 5 свечей.
    """
    analysis = None

    # Предполагается, что свечи отсортированы от старых к новым
    last_candle = candles[-1]
    prev_candle = candles[-2]

    # Определение тел свечей
    last_body = last_candle['close'] - last_candle['open']
    prev_body = prev_candle['close'] - prev_candle['open']

    logger.debug(f"Предыдущая свеча: Start={prev_candle['start']}, Open={prev_candle['open']}, "
                 f"Close={prev_candle['close']}, Body={prev_body}")
    logger.debug(f"Последняя свеча: Start={last_candle['start']}, Open={last_candle['open']}, "
                 f"Close={last_candle['close']}, Body={last_body}")

    # Проверка на бычье поглощение
    bullish_engulfing = (
        prev_body < 0 and  # Предыдущая свеча была медвежьей
        last_body > 0 and  # Последняя свеча была бычьей
        last_candle['open'] <= prev_candle['close'] and
        last_candle['close'] > prev_candle['open']
    )

    # Проверка на медвежье поглощение
    bearish_engulfing = (
        prev_body > 0 and  # Предыдущая свеча была бычьей
        last_body < 0 and  # Последняя свеча была медвежьей
        last_candle['open'] >= prev_candle['close'] and
        last_candle['close'] < prev_candle['open']
    )

    logger.debug(f"Bullish Engulfing: {bullish_engulfing}")
    logger.debug(f"Bearish Engulfing: {bearish_engulfing}")

    if bullish_engulfing:
        analysis = 'long'
        logger.info(f"Сигнал: LONG (Engulfing) для свечи {last_candle['start']}")
    elif bearish_engulfing:
        analysis = 'short'
        logger.info(f"Сигнал: SHORT (Engulfing) для свечи {last_candle['start']}")

    return analysis
