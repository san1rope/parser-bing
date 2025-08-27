import os
from logging import Logger
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from models import ProxyData

load_dotenv()


class Config:
    HEADLESS = bool(int(os.getenv("HEADLESS").strip()))
    MAX_BROWSERS = int(os.getenv("MAX_BROWSERS").strip())
    MAX_PAGES_PER_BROWSER = int(os.getenv("MAX_PAGES_PER_BROWSER").strip())
    OUT_FILEPATH = Path(os.path.abspath(os.getenv("OUT_FILEPATH").strip()))

    logger: Optional[Logger] = None
    LOGGING_DIR = Path(os.path.abspath("log"))
    DATETIME_FORMAT = os.getenv("DATETIME_FORMAT").strip()

    QUERIES_FILEPATH = Path(os.path.abspath("queries.txt"))
    PROXIES_FILEPATH = Path(os.path.abspath("proxies.txt"))

    @staticmethod
    async def load_queries() -> List[str]:
        Config.QUERIES_FILEPATH.parent.mkdir(exist_ok=True, parents=True)
        Config.QUERIES_FILEPATH.touch(exist_ok=True)

        with open("queries.txt", "r", encoding="utf-8") as file:
            return file.read().split("\n")

    @staticmethod
    async def load_proxies() -> List[ProxyData]:
        Config.PROXIES_FILEPATH.parent.mkdir(exist_ok=True, parents=True)
        Config.PROXIES_FILEPATH.touch(exist_ok=True)

        with open("proxies.txt", "r", encoding="utf-8") as file:
            proxies = file.read().split("\n")
            proxies_objs = []
            for proxy, proxy_id in zip(proxies, range(1, len(proxies) + 1)):
                host, port, username, password = proxy.split(":")
                proxies_objs.append(ProxyData(id=proxy_id, host=host, port=port, username=username, password=password))

        return proxies_objs
