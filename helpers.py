# helpers.py
import logging
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

def analyze_candles(candles: List[Dict[str, float]]) -> Dict[str, Optional[str]]:
    """
    Анализирует свечи для определения сигнала лонг или шорт на основе общего движения и модели поглощения.

    :param candles: Список свечей, каждая свеча представлена словарём с ключами 'start', 'open', 'high', 'low', 'close', 'volume'.
    :return: Словарь с ключом 'signal', значением 'long', 'short' или None.
    """
    analysis = {'signal': None}

    if len(candles) < 2:
        logger.warning("Недостаточно свечей для анализа поглощения.")
        return analysis

    logger.debug("Полученные свечи:")
    for idx, candle in enumerate(candles, start=1):
        logger.debug(f"Candle {idx}: Start={candle['start']}, Open={candle['open']}, High={candle['high']}, "
                     f"Low={candle['low']}, Close={candle['close']}, Volume={candle['volume']}")

    total_movement = sum(candle['close'] - candle['open'] for candle in candles[:-1])
    logger.debug(f"Общее движение свечей (без последней): {total_movement}")

    if total_movement > 0:
        trend = 'uptrend'
    elif total_movement < 0:
        trend = 'downtrend'
    else:
        trend = 'sideways'

    logger.debug(f"Направление тренда: {trend}")

    last_candle = candles[-1]
    prev_candle = candles[-2]

    last_body = last_candle['close'] - last_candle['open']
    prev_body = prev_candle['close'] - prev_candle['open']

    logger.debug(f"Предыдущая свеча: Start={prev_candle['start']}, Open={prev_candle['open']}, Close={prev_candle['close']}, Body={prev_body}")
    logger.debug(f"Последняя свеча: Start={last_candle['start']}, Open={last_candle['open']}, Close={last_candle['close']}, Body={last_body}")

    bullish_engulfing = (
        prev_body < 0 and
        last_body > 0 and
        last_candle['open'] <= prev_candle['close'] and
        last_candle['close'] > prev_candle['open']
    )

    bearish_engulfing = (
        prev_body > 0 and
        last_body < 0 and
        last_candle['open'] >= prev_candle['close'] and
        last_candle['close'] < prev_candle['open']
    )

    logger.debug(f"Bullish Engulfing: {bullish_engulfing}")
    logger.debug(f"Bearish Engulfing: {bearish_engulfing}")

    if trend == 'downtrend' and bullish_engulfing:
        analysis['signal'] = 'long'
        logger.info(f"Сигнал: LONG для свечи {last_candle['start']}")
    elif trend == 'uptrend' and bearish_engulfing:
        analysis['signal'] = 'short'
        logger.info(f"Сигнал: SHORT для свечи {last_candle['start']}")
    else:
        logger.info("Сигнал не сформирован.")

    return analysis
