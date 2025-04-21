# ext/logging_config.py
import logging

def setup_logger(
    log_file: str = 'app.log',
    level: int = logging.INFO
) -> logging.Logger:
    """
    Настраивает и возвращает корневой логгер для проекта.
    """
    logger = logging.getLogger()
    logger.setLevel(level)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger
