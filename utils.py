import asyncio
import re
import logging
from queue import Empty
from typing import List, Dict, Optional, Union
from urllib.parse import urlparse, parse_qs, unquote, urljoin
from multiprocessing import Queue

from selectolax.parser import HTMLParser

from models import QueueMessage, SearchResult

logger = logging.getLogger(__name__)


class Utils:
    BING = "https://www.bing.com"

    GET_NEW_QUERY = "get_new_query"
    SEND_NEW_QUERY = "send_new_query"
    GET_NEW_PROXY = "get_new_proxy"
    SEND_NEW_PROXY = "send_new_proxy"
    UPLOAD_DATA = "upload_data"

    @staticmethod
    def wrapper(func, *args, **kwargs):
        return asyncio.run(func(*args, **kwargs))

    @staticmethod
    async def smooth_scroll_wheel(page, distance=1500, step=50, delay=0.03):
        scrolled = 0
        while scrolled < distance:
            await page.mouse.wheel(0, step)
            scrolled += step
            await asyncio.sleep(delay)

    @staticmethod
    async def calculate_pages_count(lst, n):
        k, m = divmod(len(lst), n)
        return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]

    @staticmethod
    async def get_message_from_queue(queue: Queue) -> Union[QueueMessage, None]:
        try:
            msg = queue.get_nowait()
            return msg

        except Empty:
            return None
