# ext/bybit_api.py
import asyncio
from typing import List, Optional
import aiohttp
from aiohttp import ClientResponseError, ContentTypeError
import logging

API_BASE_URL = "https://api.bybit.com/v5/market/"

# Используем общий логгер
logger = logging.getLogger(__name__)

async def get_usdt_perpetual_symbols(session: aiohttp.ClientSession) -> List[str]:
    """
    Получает список символов USDT Perpetual из API Bybit.

    :param session: Клиентская сессия aiohttp.
    :return: Список символов.
    """
    url = f"{API_BASE_URL}instruments-info"
    params = {
        "category": "linear"
    }
    try:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                logger.error(f"Неправильный статус ответа: {response.status}")
                return []
            data = await response.json()
            if data.get('retCode') == 0:
                symbols = [
                    symbol['symbol'] for symbol in data.get('result', {}).get('list', [])
                    if symbol.get('contractType') == 'LinearPerpetual' and
                       symbol.get('settleCoin') == 'USDT' and
                       symbol.get('status') == 'Trading'
                ]
                logger.info(f"Получено {len(symbols)} символов USDT Perpetual.")
                return symbols
            else:
                logger.error(f"Ошибка при получении списка символов: {data.get('retMsg')}")
                return []
    except Exception as e:
        logger.exception(f"Произошла ошибка при получении списка символов: {e}")
        return []

async def get_historical_kline_data(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> List[dict]:
    """
    Получает исторические данные свечей (kline) для заданного символа и интервала.

    :param session: Клиентская сессия aiohttp.
    :param symbol: Символ для получения данных.
    :param interval: Интервал свечей (например, '1', '5', '15').
    :param limit: Количество свечей.
    :return: Список свечей в виде словарей.
    """
    url = f"{API_BASE_URL}kline"
    params = {
        "category": "linear",
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    try:
        async with session.get(url, params=params) as response:
            if response.status != 200:
                logger.error(f"Неправильный статус ответа для {symbol} на интервале {interval}: {response.status}")
                return []
            data = await response.json()
            logger.debug(f"Полученные данные свечей: {data}")
            if data.get('retCode') == 0 and data.get('result', {}).get('list'):
                candle_list = data['result']['list']
                candle_list = candle_list[::-1]
                logger.info(f"Получено {len(candle_list)} свечей для {symbol} на интервале {interval}.")
                return candle_list
            else:
                if data.get('retCode') == 0:
                    logger.warning(f"Нет данных свечей для {symbol} на интервале {interval}.")
                else:
                    logger.error(f"Ошибка при получении данных свечей для {symbol}: {data.get('retMsg')}")
                return []
    except Exception as e:
        logger.exception(f"Произошла ошибка при получении данных свечей для {symbol}: {e}")
        return []

async def get_kline_with_retries(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int, retries: int = 3, delay: int = 1) -> Optional[List[dict]]:
    """
    Пытается получить данные свечей с повторными попытками в случае неудачи.

    :param session: Клиентская сессия aiohttp.
    :param symbol: Символ для получения данных.
    :param interval: Интервал свечей.
    :param limit: Количество свечей.
    :param retries: Количество повторных попыток.
    :param delay: Задержка между попытками в секундах.
    :return: Список свечей или None в случае неудачи.
    """
    for attempt in range(1, retries + 1):
        try:
            kline_data = await get_historical_kline_data(session, symbol, interval, limit)
            if kline_data:
                return kline_data
            else:
                logger.warning(f"Попытка {attempt} для {symbol} на интервале {interval} вернула пустые данные.")
        except ClientResponseError as e:
            logger.error(f"Ошибка ответа клиента для {symbol} на интервале {interval}: {e.status}, {e.message}")
        except ContentTypeError as e:
            logger.error(f"Неверный тип содержимого для {symbol} на интервале {interval}: {e}")
        except Exception as e:
            logger.exception(f"Неизвестная ошибка для {symbol} на интервале {interval}: {e}")
        
        if attempt < retries:
            logger.info(f"Повторная попытка через {delay} секунд...")
            await asyncio.sleep(delay)
    
    logger.error(f"Не удалось получить данные для {symbol} на интервале {interval} после {retries} попыток.")
    return None
