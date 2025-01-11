# ext/messaging.py
import asyncio
import logging
from telegram import Bot
from telegram.error import TelegramError, RetryAfter, TimedOut
from ext.utils import escape_markdown

async def send_telegram_message(bot: Bot, chat_id: str, message: str, logger: logging.Logger, max_attempts: int = 5, initial_delay: float = 1.0, backoff_factor: float = 2.0):
    """
    Отправляет сообщение в Telegram с обработкой ошибок и экспоненциальным откатом.

    :param bot: Экземпляр бота Telegram.
    :param chat_id: ID чата для отправки сообщения.
    :param message: Текст сообщения.
    :param logger: Логгер для записи логов.
    :param max_attempts: Максимальное количество попыток отправки.
    :param initial_delay: Начальная задержка перед первой повторной попыткой.
    :param backoff_factor: Фактор увеличения задержки при каждой повторной попытке.
    """
    escaped_message = escape_markdown(message)
    attempt = 1
    delay = initial_delay

    while attempt <= max_attempts:
        try:
            await bot.send_message(chat_id=chat_id, text=escaped_message, parse_mode='MarkdownV2', disable_web_page_preview=True)
            logger.info(f"Сообщение отправлено в Telegram: {message}")
            return
        except RetryAfter as e:
            wait_time = e.retry_after
            logger.warning(f"Flood control. Повторная попытка через {wait_time} секунд. Попытка {attempt}/{max_attempts}")
            await asyncio.sleep(wait_time)
        except TimedOut:
            logger.warning(f"Тайм-аут. Повторная попытка отправки. Попытка {attempt}/{max_attempts}")
            await asyncio.sleep(delay)
            delay *= backoff_factor
        except TelegramError as e:
            logger.error(f"Ошибка при отправке сообщения в Telegram: {e}. Попытка {attempt}/{max_attempts}")
            await asyncio.sleep(delay)
            delay *= backoff_factor
        except Exception as e:
            logger.error(f"Неизвестная ошибка при отправке сообщения в Telegram: {e}. Попытка {attempt}/{max_attempts}")
            await asyncio.sleep(delay)
            delay *= backoff_factor

        attempt += 1

    logger.error(f"Не удалось отправить сообщение после {max_attempts} попыток: {message}")


async def message_worker(bot: Bot, chat_id: str, message_queue: asyncio.Queue, logger: logging.Logger, semaphore: asyncio.Semaphore):
    """
    Обработчик очереди сообщений для отправки в Telegram.

    :param bot: Экземпляр бота Telegram.
    :param chat_id: ID чата для отправки сообщений.
    :param message_queue: Очередь сообщений.
    :param logger: Логгер для записи логов.
    :param semaphore: Семафор для ограничения одновременных отправок.
    """
    while True:
        message = await message_queue.get()
        if message == "EXIT":
            logger.info("Получен сигнал завершения отправки сообщений.")
            message_queue.task_done()
            break

        async with semaphore:
            await send_telegram_message(bot, chat_id, message, logger)

        message_queue.task_done()


async def run_message_workers(bot: Bot, chat_id: str, message_queue: asyncio.Queue, logger: logging.Logger, max_workers: int = 5):
    """
    Запускает несколько задач-воркеров для обработки очереди сообщений.

    :param bot: Экземпляр бота Telegram.
    :param chat_id: ID чата для отправки сообщений.
    :param message_queue: Очередь сообщений.
    :param logger: Логгер для записи логов.
    :param max_workers: Количество воркеров.
    """
    semaphore = asyncio.Semaphore(max_workers)
    workers = [
        asyncio.create_task(message_worker(bot, chat_id, message_queue, logger, semaphore))
        for _ in range(max_workers)
    ]
    await asyncio.gather(*workers)
