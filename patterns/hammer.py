# hammer.py
import logging
from typing import List, Dict, Optional, Any

logger = logging.getLogger(__name__)

def is_hammer(candle: Dict[str, float], direction: str) -> bool:
    """
    Определяет, является ли свеча паттерном "молот" или "перевернутый молот" в зависимости от направления.
    """
    try:
        open_price = float(candle['open'])
        close_price = float(candle['close'])
        high = float(candle['high'])
        low = float(candle['low'])
    except (ValueError, KeyError) as e:
        logger.error(f"Ошибка при чтении данных свечи: {e}")
        return False

    body = abs(close_price - open_price)
    lower_wick = min(close_price, open_price) - low
    upper_wick = high - max(close_price, open_price)

    if direction == 'down':
        return lower_wick > 2 * body and upper_wick < body
    elif direction == 'up':
        return upper_wick > 2 * body and lower_wick < body
    else:
        return False

def determine_overall_direction(candles: List[Dict[str, float]]) -> str:
    """
    Определяет направление движения:
      - 'up' если закрытие последней свечи > открытия первой свечи,
      - 'down' если закрытие последней свечи < открытия первой свечи,
      - 'neutral' в остальных случаях.
    """
    if not candles or len(candles) < 2:
        return 'neutral'
    try:
        first_open = float(candles[0]['open'])
        last_close = float(candles[-1]['close'])
    except (ValueError, KeyError) as e:
        logger.error(f"Ошибка при определении направления: {e}")
        return 'neutral'
    if last_close > first_open:
        return 'up'
    elif last_close < first_open:
        return 'down'
    else:
        return 'neutral'

def find_extreme_point(candles: List[Dict[str, float]], direction: str) -> Optional[float]:
    """
    Находит экстремальное значение среди свечей:
      - Максимум 'high' для направления 'up',
      - Минимум 'low' для направления 'down'.
    """
    try:
        if direction == 'up':
            return max(float(candle['high']) for candle in candles)
        elif direction == 'down':
            return min(float(candle['low']) for candle in candles)
        else:
            return None
    except (ValueError, KeyError) as e:
        logger.error(f"Ошибка при поиске экстремума: {e}")
        return None

def analyze_hammer(candles: List[Dict[str, float]]) -> Dict[str, Optional[Any]]:
    """
    Анализирует паттерн "молот" и "перевернутый молот" для последних 5 свечей.
    
    Предполагается, что анализируются только 5 последних свечей.
    Из них последняя свеча может быть ещё открытой, поэтому анализ проводится по 4 закрытым свечам.
    """
    if len(candles) < 5:
        logger.warning("Для анализа паттерна 'молот' требуется минимум 5 свечей.")
        return {'hammer_condition': False, 'direction': None}

    relevant_candles = candles[-5:]
    
    closed_candles = relevant_candles[:-1]
    if len(closed_candles) < 2:
        return {'hammer_condition': False, 'direction': None}
    
    hammer_candle = closed_candles[-1]
    previous_candles = closed_candles[:-1]
    
    direction = determine_overall_direction(closed_candles)
    is_hammer_pattern = is_hammer(hammer_candle, direction)
    
    if direction in ['up', 'down'] and previous_candles:
        extreme_point = find_extreme_point(previous_candles, direction)
        if direction == 'up':
            # Для перевернутого молота: верхняя точка свечи должна превышать экстремум предыдущих свечей
            has_higher_high = float(hammer_candle['high']) > extreme_point if extreme_point is not None else False
            upper_wick = float(hammer_candle['high']) - max(float(hammer_candle['close']), float(hammer_candle['open']))
            has_upper_wick = upper_wick > 0
            hammer_condition = is_hammer_pattern and has_higher_high and has_upper_wick
        elif direction == 'down':
            # Для обычного молота: нижняя точка свечи должна быть ниже экстремума предыдущих свечей
            has_lower_low = float(hammer_candle['low']) < extreme_point if extreme_point is not None else False
            lower_wick = min(float(hammer_candle['close']), float(hammer_candle['open'])) - float(hammer_candle['low'])
            has_lower_wick = lower_wick > 0
            hammer_condition = is_hammer_pattern and has_lower_low and has_lower_wick
        else:
            hammer_condition = False
    else:
        hammer_condition = False

    return {'hammer_condition': hammer_condition, 'direction': direction if hammer_condition else None}
