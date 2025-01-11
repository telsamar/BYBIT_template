#  ext/utils.py
import re
from typing import Pattern

# Константа со всеми символами, которые необходимо экранировать в MarkdownV2
ESCAPE_CHARS = r'_*[]()~`>#+\-=\|{}.!'

# Предварительная компиляция регулярного выражения для повышения производительности
_ESCAPE_MARKDOWN_PATTERN: Pattern = re.compile(f'([{re.escape(ESCAPE_CHARS)}])')

def escape_markdown(text: str) -> str:
    """
    Экранирует символы, используемые в MarkdownV2.

    :param text: Текст для экранирования.
    :return: Экранированный текст.
    """
    return _ESCAPE_MARKDOWN_PATTERN.sub(r'\\\1', text)
