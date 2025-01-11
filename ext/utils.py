#  ext/utils.py
import re
from typing import Pattern

ESCAPE_CHARS = r'_*[]()~`>#+\-=\|{}.!'

_ESCAPE_MARKDOWN_PATTERN: Pattern = re.compile(f'([{re.escape(ESCAPE_CHARS)}])')

def escape_markdown(text: str) -> str:
    """
    Экранирует символы, используемые в MarkdownV2.

    :param text: Текст для экранирования.
    :return: Экранированный текст.
    """
    return _ESCAPE_MARKDOWN_PATTERN.sub(r'\\\1', text)
