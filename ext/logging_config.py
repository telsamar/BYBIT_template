# ext/logging_config.py
import logging

def setup_logger(
    log_file: str = 'app.log',
    level: int = logging.DEBUG
) -> logging.Logger:
    """
    Настраивает и возвращает корневой логгер для проекта.

    :param log_file: Путь к файлу лога.
    :param level: Уровень логирования.
    :return: Настроенный корневой логгер.
    """
    logger = logging.getLogger()  # Получаем корневой логгер
    logger.setLevel(level)
    
    # Удаляем существующие обработчики, чтобы избежать дублирования логов при повторных вызовах
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Форматтер для логов
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Создаём FileHandler для записи логов в файл с перезаписью
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger
