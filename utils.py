import asyncio
import os
import logging
from datetime import datetime
from logging import Logger
from pathlib import Path
from queue import Empty
from typing import Union
from multiprocessing import Queue

from config import Config
from models import QueueMessage


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

    @staticmethod
    async def add_logging(process_id: int, datetime_of_start: Union[datetime, str]) -> Logger:
        if isinstance(datetime_of_start, str):
            file_dir = datetime_of_start

        elif isinstance(datetime_of_start, datetime):
            file_dir = datetime_of_start.strftime(Config.DATETIME_FORMAT)

        else:
            raise TypeError("datetime_of_start must be str or datetime")

        log_filepath = Path(os.path.abspath(f"{Config.LOGGING_DIR}/{file_dir}/{process_id}.txt"))
        log_filepath.parent.mkdir(parents=True, exist_ok=True)
        log_filepath.touch(exist_ok=True)

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - ' + str(
            process_id) + '| %(message)s')

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        file_handler = logging.FileHandler(log_filepath, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        return logger
