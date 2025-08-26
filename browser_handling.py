import asyncio
import base64
import logging
import time
import re
from random import uniform, randint
from typing import List, Optional, Union
from multiprocessing import Queue
from urllib.parse import urlparse, parse_qs, unquote, urljoin

from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth import Stealth
from playwright._impl._errors import TimeoutError
from selectolax.parser import HTMLParser

from config import Config
from models import QueueMessage, ProxyData, SearchResult
from utils import Utils as Ut

logger = logging.getLogger(__name__)


class ParserTask:

    def __init__(self, queue_in: Queue, queue_out: Queue, pages_count: int):
        logging.basicConfig(level=logging.INFO,
                            format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s')
        logger.info("Новый процесс!")

        self.queue_in: Queue = queue_in
        self.queue_out: Queue = queue_out
        self.pages_count: int = pages_count

        self.proxy: Optional[ProxyData] = None
        self.playwright_obj = None
        self.browser: Optional[Browser] = None
        self.context = None
        self.all_pages: List[Optional[Page]] = []

        asyncio.run(self.run_tasks())

    async def run_tasks(self):
        await self.get_new_browser_obj()

        tasks = []
        for page in self.all_pages:
            tasks.append(self.queries_iteration(page=page))

        await asyncio.gather(*tasks)

    async def get_new_proxy(self) -> bool:
        self.queue_out.put(QueueMessage(msg_type=Ut.GET_NEW_PROXY, data=self.proxy))

        while True:
            msg = await Ut.get_message_from_queue(queue=self.queue_in)
            if msg is None:
                continue

            if msg.msg_type == Ut.SEND_NEW_PROXY:
                self.proxy = msg.data
                return True

            time.sleep(0.1)

    async def get_new_query(self) -> str:
        self.queue_out.put(QueueMessage(msg_type=Ut.GET_NEW_QUERY))

        while True:
            msg = await Ut.get_message_from_queue(queue=self.queue_in)
            if msg is None:
                continue

            if msg.msg_type == Ut.SEND_NEW_QUERY:
                return msg.data

            time.sleep(0.1)

    async def send_data_to_file(self, search_results: List[SearchResult]):
        self.queue_out.put(QueueMessage(msg_type=Ut.UPLOAD_DATA, data=search_results))

    async def get_new_browser_obj(self) -> bool:
        if self.browser is not None:
            await self.browser.close()

        if self.playwright_obj is not None:
            await self.playwright_obj.stop()

        self.playwright_obj = await async_playwright().start()

        if self.proxy is None:
            await self.get_new_proxy()

        self.browser = await self.playwright_obj.chromium.launch(
            headless=Config.HEADLESS,
            proxy={
                "server": f"http://{self.proxy.host}:{self.proxy.port}",
                "username": self.proxy.username,
                "password": self.proxy.password
            }
        )
        self.context = await self.browser.new_context(
            locale="ru-RU",
            extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9"},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.6367.207 Safari/537.36"
        )

        for n in range(self.pages_count):
            self.all_pages.append(await self.context.new_page())

        return True

    @staticmethod
    async def clean_bing_href(href: str) -> Union[str, None]:
        if not href or href.startswith("javascript:") or href == "#":
            return None

        if href.startswith("/"):
            href = urljoin(Ut.BING, href)

        p = urlparse(href)
        if p.netloc and not p.netloc.endswith("bing.com") and p.netloc != "go.microsoft.com":
            return href

        qs = parse_qs(p.query)
        for key in ("u", "r", "url", "target", "mediaurl"):
            val = qs.get(key) or qs.get(key.upper())
            if val and val[0]:
                url = unquote(val[0])
                if "%2F" in url or "%3A" in url:
                    url = unquote(url)

                return url

        return None

    async def _text(self, el) -> str:
        return re.sub(r"\s+", " ", el.text(strip=True)) if el else ""

    async def parse_data(
            self, page: Page, count_of_page: int, count_of_result: int, user_agent: str, max_snippet_len: int = 300
    ) -> List[SearchResult]:
        tree = HTMLParser(await page.content())
        out = []

        lang = await page.evaluate("document.documentElement.lang")

        container = tree.css_first("#b_results") or tree
        items = container.css("li.b_algo")

        for li in items:
            a = li.css_first("h2 a") or li.css_first("h3 a") or li.css_first(".b_title a")
            if not a:
                continue

            raw_href = a.attrs.get("href", "")
            url = await self.clean_bing_href(raw_href) or (raw_href if raw_href else None)
            if not url:
                continue

            title = await self._text(a)
            if not title:
                continue

            snippet = ""
            for sel in (".b_caption p", ".b_snippet", ".b_desc", ".b_caption", "p"):
                el = li.css_first(sel)
                snippet = await self._text(el)
                if snippet and len(snippet) > 10:
                    break

            if max_snippet_len and snippet:
                snippet = snippet[:max_snippet_len].rstrip()

            count_of_result += 1

            if url.startswith("a1"):
                url = url[2:]
                url = url + "=" * (-len(url) % 4)
                try:
                    url = base64.b64decode(url)

                except Exception:
                    url = base64.urlsafe_b64decode(url)

                url = url.decode("utf-8")

            out.append(SearchResult(
                query=page.url,
                page=str(count_of_page),
                position=str(count_of_result),
                title=title,
                url=url,
                snippet=snippet,
                lang=lang,
                mkt="ru-RU",
                country="ru",
                user_agent=user_agent,
                proxy=self.proxy.__str__()
            ).to_list())

        await self.send_data_to_file(search_results=out)
        return out

    async def queries_iteration(self, page: Page):
        await Stealth().apply_stealth_async(page)

        await page.goto(Ut.BING)

        while True:
            count_of_page = 0
            count_of_result = 0

            query = await self.get_new_query()
            if query is None:
                logger.info("Новых запросов не поступило! Задача закончила свою работу.")
                return

            await page.click("#sb_form_q", timeout=10000)
            await asyncio.sleep(uniform(0.1, 0.5))
            await page.fill("#sb_form_q", "")
            await asyncio.sleep(uniform(0.1, 0.5))
            await page.fill("#sb_form_q", query)
            await asyncio.sleep(uniform(0.1, 0.5))
            await page.locator("#sb_form").evaluate("form => form.submit()")

            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36"

            while True:
                flag = False
                for _ in range(2):
                    try:
                        await page.wait_for_selector("li.b_algo h2 a", timeout=5000)
                        flag = True

                    except TimeoutError:
                        await page.screenshot(path="temp.png")
                        print("page reload")
                        await page.reload()
                        continue

                if not flag:
                    break

                await asyncio.sleep(uniform(0.5, 1))
                await Ut.smooth_scroll_wheel(page=page, distance=randint(200, 500))

                count_of_page += 1

                parsed_data = await self.parse_data(page=page, count_of_page=count_of_page, user_agent=user_agent,
                                                    count_of_result=count_of_result)
                count_of_result += len(parsed_data)

                await Ut.smooth_scroll_wheel(page=page, distance=2000)
                await asyncio.sleep(uniform(0.2, 0.5))

                await page.wait_for_selector(".sb_pagN", timeout=5000)

                await page.click(".sb_pagN")
