# main.py
import asyncio
import os
import aiohttp
from datetime import datetime
from dotenv import load_dotenv
from telegram import Bot
from ext.bybit_api import get_usdt_perpetual_symbols, get_kline_with_retries
from helpers import analyze_candles
from ext.messaging import run_message_workers, send_telegram_message
from ext.logging_config import setup_logger
from collections import defaultdict
from typing import Dict, Any, List, Optional
import logging 

class SharedState:
    def __init__(self, message_limit: int):
        self.message_limit: int = message_limit
        self.messages_sent: int = 0
        self.lock: asyncio.Lock = asyncio.Lock()
        self.limit_reached_event: asyncio.Event = asyncio.Event()

load_dotenv()
BOT_TOKEN = os.getenv('BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

MESSAGE_LIMIT = 20
MANUAL_RUN = os.getenv('MANUAL_RUN', 'false').lower() == 'true'

if not BOT_TOKEN or not CHAT_ID:
    raise ValueError("BOT_TOKEN и CHAT_ID должны быть установлены в .env файле.")

setup_logger(log_file='app.log', level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_and_analyze(
    symbol: str,
    interval_key: str,
    interval_value: str,
    session: aiohttp.ClientSession,
    signals: defaultdict,
    shared_state: SharedState
) -> None:
    """
    Получает данные свечей и анализирует их для заданного символа и интервала.
    """
    if shared_state.limit_reached_event.is_set():
        return

    try:
        kline_data = await get_kline_with_retries(session, symbol, interval_key, limit=36)
        if not kline_data:
            logger.warning(f"Нет данных свечей для {symbol} на интервале {interval_value}.")
            return

        fields = ['start', 'open', 'high', 'low', 'close', 'volume']
        
        candles = []
        for candle in kline_data[:-1]:
            candle_dict = dict(zip(fields, candle))
            try:
                candle_dict['open'] = float(candle_dict['open'])
                candle_dict['high'] = float(candle_dict['high'])
                candle_dict['low'] = float(candle_dict['low'])
                candle_dict['close'] = float(candle_dict['close'])
                candle_dict['volume'] = float(candle_dict['volume'])
            except ValueError as ve:
                logger.error(f"Ошибка конвертации числовых значений для {symbol}: {ve}")
                continue
            candles.append(candle_dict)

        logger.debug(f"Преобразованные свечи для {symbol}: {candles}")

        for candle in candles:
            if not all(key in candle for key in ['start', 'open', 'high', 'low', 'close', 'volume']):
                logger.error(f"Недостающие ключи в свече для {symbol}: {candle}")
                return
            if not all(isinstance(candle[key], (int, float)) for key in ['open', 'high', 'low', 'close', 'volume']):
                logger.error(f"Некорректные типы данных в свече для {symbol}: {candle}")
                return

        analysis = analyze_candles(candles)

        signal = analysis.get('signal')

        if signal:
            trade_action = "SHORT" if signal == "short" else "LONG"
            signals[trade_action].append({
                "interval": interval_value,
                "k":    analysis["%K"],
                "d":    analysis["%D"],
                "macd": analysis["MACD"],
            })

    except aiohttp.ClientResponseError as e:
        logger.error(f"Ошибка при получении данных свечей для {symbol}: {e.status}, {e.message}, URL: {e.request_info.url}")
    except aiohttp.ContentTypeError as e:
        logger.error(f"Неверный тип содержимого при получении данных для {symbol}: {e}")
    except Exception as e:
        logger.error(f"Ошибка при обработке символа {symbol} на интервале {interval_value}: {e}")

async def process_symbol(
    symbol: str,
    intervals: Dict[str, str],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    message_queue: asyncio.Queue,
    shared_state: SharedState
) -> None:
    """
    Обрабатывает символ на указанных интервалах и объединяет сигналы для одного символа.
    """
    async with semaphore:
        if shared_state.limit_reached_event.is_set():
            return

        signals = defaultdict(list)
        tasks = [
            fetch_and_analyze(symbol, interval_key, interval_value, session, signals, shared_state)
            for interval_key, interval_value in intervals.items()
        ]

        await asyncio.gather(*tasks)

        if not signals:
            return

        message_lines = [f"#{symbol}"]
        for action, entries in signals.items():
            emoji = "🟢" if action == "LONG" else "🔴"
            for e in entries:
                iv = e["interval"]
                k  = e["k"]
                d  = e["d"]
                m  = e["macd"]
                message_lines.append(
                    f"{emoji} {action} {iv} — K={k:.2f}, D={d:.2f}, MACD={m:.6f}"
                )

        message = "\n".join(message_lines) + "\n"
    await message_queue.put(message)

async def main() -> None:
    """
    Основная функция приложения.

    Загружает символы, определяет интервалы, запускает обработку символов и воркеров для отправки сообщений.
    """
    is_manual_run = MANUAL_RUN
    current_time = datetime.now()
    current_minute = current_time.minute
    current_hour = current_time.hour

    intervals = {
        # '5': '5m',
        # '15': '15m',
        '30': '30m',
        '60': '1h',
        '240': '4h',
        # '720': '12h',
    }

    if not is_manual_run:
        if current_minute % 60 == 0:
            if current_hour % 12 == 0 and current_minute == 0:
                intervals = {k: v for k, v in intervals.items() if k in ['5', '15', '30', '60', '240', '720', '1440']}
            elif current_hour % 4 == 0 and current_minute == 0:
                intervals = {k: v for k, v in intervals.items() if k in ['5', '15', '30', '60', '240']}
            else:
                intervals = {k: v for k, v in intervals.items() if k in ['5', '15', '30', '60']}
        elif current_minute % 30 == 0:
            intervals = {k: v for k, v in intervals.items() if k in ['5', '15', '30']}
        elif current_minute % 15 == 0:
            intervals = {k: v for k, v in intervals.items() if k in ['5', '15']}
        elif current_minute % 5 == 0:
            intervals = {k: v for k, v in intervals.items() if k in ['5']}
        else:
            logger.info("Запуск в неправильное время.")
            return

    MAX_CONCURRENT_TASKS = 50
    MAX_WORKERS = 15
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    message_queue = asyncio.Queue()

    shared_state = SharedState(message_limit=MESSAGE_LIMIT)

    async with aiohttp.ClientSession() as session:
        excluded_symbols = {'USDCUSDT', 'FDUSDUSDT'}

        symbols = await get_usdt_perpetual_symbols(session)
        symbols = [symbol for symbol in symbols if symbol.upper() not in excluded_symbols]

        if not symbols:
            async with Bot(token=BOT_TOKEN) as bot:
                await send_telegram_message(bot, CHAT_ID, "❌ Список символов пуст.", logger)
            return

        # Если хотите протестировать на конкретном символе, раскомментируйте блок ниже
        # if len(symbols) >= 10:
        #     test_symbol = symbols[9]  # Индексация с 0
        #     symbols = [test_symbol]
        #     logger.info(f"Тестирование на символе: {test_symbol}")
        # else:
        #     logger.warning("В списке символов меньше 10 элементов. Будет обработан первый символ.")
        #     symbols = [symbols[0]]

        # Инициализация бота внутри main для правильного управления жизненным циклом
        async with Bot(token=BOT_TOKEN) as bot:
            worker_task = asyncio.create_task(
                run_message_workers(
                    bot,
                    CHAT_ID,
                    message_queue,
                    logger,
                    max_workers=MAX_WORKERS
                )
            )

            tasks = [
                process_symbol(symbol, intervals, session, semaphore, message_queue, shared_state)
                for symbol in symbols
            ]

            await asyncio.gather(*tasks)

            for _ in range(MAX_WORKERS):
                await message_queue.put("EXIT")

            await worker_task

if __name__ == "__main__":
    setup_logger(log_file='app.log', level=logging.INFO)
    asyncio.run(main())
