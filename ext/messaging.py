# ext/messaging.py

import os
import asyncio
import time
from collections import deque
import logging

from telegram import Bot
from telegram.error import RetryAfter, TimedOut, TelegramError

from ext.utils import escape_markdown

MESSAGES_PER_MINUTE = int(os.getenv("MESSAGE_RATE_LIMIT", 20))
_WINDOW_SECONDS = 60 

_sent_timestamps: deque[float] = deque()

async def send_telegram_message(
    bot: Bot,
    chat_id: str,
    message: str,
    logger: logging.Logger,
    max_attempts: int = 5,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0
) -> None:
    """
    Отправляет одно текстовое сообщение с экранированием MarkdownV2 и
    экспоненциальным бэкоффом при ошибках.
    """
    text = escape_markdown(message)
    attempt = 1
    delay = initial_delay

    while attempt <= max_attempts:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True
            )
            logger.info(f"Сообщение отправлено: {message.strip()}")
            return
        except RetryAfter as e:
            logger.warning(f"Flood control: ждём {e.retry_after}s (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            logger.warning(f"Таймаут отправки (попытка {attempt}/{max_attempts}), ждём {delay:.1f}s")
            await asyncio.sleep(delay)
            delay *= backoff_factor
        except TelegramError as e:
            logger.error(f"Ошибка Telegram API: {e} (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(delay)
            delay *= backoff_factor
        except Exception as e:
            logger.error(f"Неизвестная ошибка: {e} (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(delay)
            delay *= backoff_factor

        attempt += 1

    logger.error(f"Не удалось отправить сообщение после {max_attempts} попыток: {message.strip()}")


async def message_worker(
    bot: Bot,
    chat_id: str,
    message_queue: asyncio.Queue,
    logger: logging.Logger
) -> None:
    """
    Воркер, обрабатывающий очередь: извлекает сообщения и отправляет их
    с учётом RATE LIMIT.
    """
    while True:
        message = await message_queue.get()
        if message == "EXIT":
            logger.info("Воркер получил сигнал завершения.")
            message_queue.task_done()
            break

        # --- RATE LIMITING ---
        now = time.time()
        while _sent_timestamps and now - _sent_timestamps[0] >= _WINDOW_SECONDS:
            _sent_timestamps.popleft()

        if len(_sent_timestamps) >= MESSAGES_PER_MINUTE:
            wait = _WINDOW_SECONDS - (now - _sent_timestamps[0])
            logger.info(f"Достигнут лимит {MESSAGES_PER_MINUTE}/минуту. Жду {wait:.1f}s.")
            await asyncio.sleep(wait)
            now = time.time()
            while _sent_timestamps and now - _sent_timestamps[0] >= _WINDOW_SECONDS:
                _sent_timestamps.popleft()

        try:
            await send_telegram_message(bot, chat_id, message, logger)
            _sent_timestamps.append(time.time())
        except Exception as e:
            logger.error(f"Ошибка при send_telegram_message: {e}")
        finally:
            message_queue.task_done()


async def run_message_workers(
    bot: Bot,
    chat_id: str,
    message_queue: asyncio.Queue,
    logger: logging.Logger,
    max_workers: int = 1
) -> None:
    """
    Запускает max_workers параллельных message_worker-ов и ждёт их завершения.
    """
    workers = [
        asyncio.create_task(message_worker(bot, chat_id, message_queue, logger))
        for _ in range(max_workers)
    ]
    await asyncio.gather(*workers)
