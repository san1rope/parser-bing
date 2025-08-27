import asyncio
import base64
import time
import re
import traceback
from random import uniform, randint
from typing import List, Optional, Union, Dict, Any
from multiprocessing import Queue
from urllib.parse import urlparse, parse_qs, unquote, urljoin

from playwright.async_api import async_playwright, Page, Browser
from playwright_stealth import Stealth
from playwright._impl._errors import TimeoutError, TargetClosedError
from selectolax.parser import HTMLParser

from config import Config
from models import QueueMessage, ProxyData, SearchResult
from test import logger
from utils import Utils as Ut


class ParserTask:
    C_PAGE = "c_page"
    C_QUERY = "c_query"
    C_URL = "c_url"
    COUNT_OF_PAGE = "count_of_page"
    COUNT_OF_RESULT = "count_of_result"

    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.207 Safari/537.36"

    def __init__(self, queue_in: Queue, queue_out: Queue, pages_count: int, process_id: int, datetime_of_start: str):
        self.queue_in: Queue = queue_in
        self.queue_out: Queue = queue_out
        self.pages_count: int = pages_count
        self.process_id: int = process_id
        self.datetime_of_start: str = datetime_of_start

        self.proxy: Optional[ProxyData] = None
        self.playwright_obj = None
        self.browser: Optional[Browser] = None
        self.context = None
        self.all_pages: Dict[int, Dict[str, Any]] = {}

        asyncio.run(self.run_tasks())

    async def run_tasks(self):
        new_logger = await Ut.add_logging(process_id=self.process_id, datetime_of_start=self.datetime_of_start)
        Config.logger = new_logger

        Config.logger.info("Был успешно запущен новый процесс!")

        await self.get_new_browser_obj()

        tasks = []
        for page_id in self.all_pages:
            tasks.append(self.queries_iteration_wrapper(page_id=page_id))

        await asyncio.gather(*tasks)

    async def get_new_proxy(self) -> bool:
        self.queue_out.put(QueueMessage(msg_type=Ut.GET_NEW_PROXY, data=self.proxy))
        Config.logger.info("Запросил новый прокси...")

        while True:
            msg = await Ut.get_message_from_queue(queue=self.queue_in)
            if msg is None:
                continue

            if msg.msg_type == Ut.SEND_NEW_PROXY:
                self.proxy = msg.data
                Config.logger.info(f"Получил новый прокси: {self.proxy.__str__}")
                return True

            time.sleep(0.1)

    async def get_new_query(self, page_id: int) -> bool:
        self.queue_out.put(QueueMessage(msg_type=Ut.GET_NEW_QUERY))
        Config.logger.info("Запросил новый поисковый запрос...")

        while True:
            msg = await Ut.get_message_from_queue(queue=self.queue_in)
            if msg is None:
                continue

            if msg.msg_type == Ut.SEND_NEW_QUERY:
                self.all_pages[page_id][self.C_QUERY] = msg.data
                Config.logger.info(f"Получил новый поисковый запрос: {msg.data}")
                return True

            time.sleep(0.1)

    async def send_data_to_file(self, search_results: List[SearchResult]):
        self.queue_out.put(QueueMessage(msg_type=Ut.UPLOAD_DATA, data=search_results))
        Config.logger.info("Отправил данные на выгрузку в файл!")

    async def get_new_browser_obj(self) -> bool:
        Config.logger.info("Пробую получить новый браузер...")

        if self.browser is not None:
            await self.browser.close()
            Config.logger.info("Закрыл старый браузер")

        if self.playwright_obj is not None:
            await self.playwright_obj.stop()
            Config.logger.info("Остановил старый playwright_obj")

        self.playwright_obj = await async_playwright().start()
        Config.logger.info("Запустил новый экземпляр playwright_obj")

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
        Config.logger.info("Запустил новый браузер")

        self.context = await self.browser.new_context(
            locale="ru-RU",
            extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9"},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.6367.207 Safari/537.36"
        )
        Config.logger.info("Запустил новый контекст браузера")

        for n in range(self.pages_count):
            new_page = await self.context.new_page()
            if n in self.all_pages:
                # при запросі всього сразу - капча
                # if self.all_pages[n][self.C_URL] is not None:
                #     await new_page.goto(self.all_pages[n][self.C_URL])

                self.all_pages[n][self.C_PAGE] = new_page

            else:
                self.all_pages[n] = {
                    self.C_PAGE: new_page, self.C_URL: None, self.C_QUERY: None,
                    self.COUNT_OF_PAGE: 0, self.COUNT_OF_RESULT: 0
                }

            Config.logger.info(f"Открыл новую вкладку: {n}")

        return True

    async def make_search_query(self, page_id: int, retries: int = 3):
        page: Page = await self.get_lambda_c_page(page_id=page_id)
        query: str = await self.get_lambda_c_query(page_id=page_id)

        try:
            await page().goto(Ut.BING)  # temp

            await page().click("#sb_form_q", timeout=10000)
            await asyncio.sleep(uniform(0.1, 0.5))
            await page().fill("#sb_form_q", "")
            await asyncio.sleep(uniform(0.1, 0.5))
            await page().fill("#sb_form_q", query())
            await asyncio.sleep(uniform(0.1, 0.5))
            await page().locator("#sb_form").evaluate("form => form.submit()")

            return

        except TargetClosedError:
            logger.error("Браузер был закрыт! Запускаю заново...")

            await self.get_new_browser_obj()

        except TimeoutError:
            if await self.check_bnp_container(page_id=page_id):
                Config.logger.info("Кликнул по кнопке принятия куков!")

        if retries:
            logger.info(f"Пробую заново выполнить поиск по запросу: {query()}. Попыток: {retries}")
            return await self.make_search_query(page_id=page_id, retries=retries - 1)

        else:
            logger.error(f"Не удалось выполнить поиск по запросу {query()}!")

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
            self, page_id: int, count_of_page: int, count_of_result: int, max_snippet_len: int = 300
    ) -> List[SearchResult]:
        page: Page = await self.get_lambda_c_page(page_id=page_id)

        tree = HTMLParser(await page().content())
        out = []

        lang = await page().evaluate("document.documentElement.lang")

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
                query=page().url,
                page=str(count_of_page),
                position=str(count_of_result),
                title=title,
                url=url,
                snippet=snippet,
                lang=lang,
                mkt="ru-RU",
                country="ru",
                user_agent=self.USER_AGENT,
                proxy=self.proxy.__str__()
            ).to_list())
            logger.info(f"Собрал результат! Позиция: {count_of_result} | {self.all_pages[page_id][self.C_QUERY]}")

        await self.send_data_to_file(search_results=out)
        return out

    async def smooth_scroll_wheel(self, page_id: int, distance=1500, step=50, delay=0.03):
        page: Page = await self.get_lambda_c_page(page_id=page_id)

        scrolled = 0
        while scrolled < distance:
            await page().mouse.wheel(0, step)
            scrolled += step
            await asyncio.sleep(delay)

    async def queries_iteration_wrapper(self, page_id: int):
        try:
            return await self.queries_iteration(page_id=page_id)

        except TimeoutError:
            if await self.check_bnp_container(page_id=page_id):
                Config.logger.info("Кликнул по кнопке принятия куков!")

        except Exception:
            logger.critical(traceback.format_exc())

        return await self.queries_iteration_wrapper(page_id=page_id)

    async def queries_iteration(self, page_id: int):
        page: Page = await self.get_lambda_c_page(page_id=page_id)
        query = await self.get_lambda_c_query(page_id=page_id)

        await Stealth().apply_stealth_async(page())

        await page().goto(Ut.BING)

        while True:
            await self.get_new_query(page_id=page_id)
            if query() is None:
                Config.logger.info("Новых запросов не поступило! Задача закончила свою работу.")
                return

            await self.make_search_query(page_id=page_id)

            while True:
                try:
                    if await self.collect_unparsed_data(page_id=page_id):
                        break

                except TargetClosedError:
                    logger.error("Браузер был закрыт! Запускаю заново...")

                    await self.get_new_browser_obj()

                except TimeoutError:
                    if await self.check_bnp_container(page_id=page_id):
                        Config.logger.info("Кликнул по кнопке принятия куков!")

    async def check_bnp_container(self, page_id: int) -> bool:
        page = await self.get_lambda_c_page(page_id=page_id)

        container = page().locator("#bnp_container")
        if await container.count() > 0 and await container.is_visible():
            logger.info("Нашел контейнер с куками!")
            await page().locator("#bnp_btn_accept").click()
            return True

        else:
            return False

    async def collect_unparsed_data(self, page_id: int):
        page: Page = await self.get_lambda_c_page(page_id=page_id)

        self.all_pages[page_id][self.C_URL] = page().url

        flag = False
        for n in range(3):
            try:
                await page().wait_for_selector("li.b_algo h2 a", timeout=5000)
                logger.info("Нашел результаты на странице! Собираю их...")
                flag = True

            except TimeoutError:
                logger.warning(f"Не нашел данных на странице! Попыток: {n}")
                await page().reload()
                continue

        if not flag:
            return True

        await asyncio.sleep(uniform(0.5, 1))
        await self.smooth_scroll_wheel(page_id=page_id, distance=randint(200, 500))

        self.all_pages[page_id][self.COUNT_OF_PAGE] += 1

        parsed_data = await self.parse_data(page_id=page_id, count_of_page=self.all_pages[page_id][self.COUNT_OF_PAGE],
                                            count_of_result=self.all_pages[page_id][self.COUNT_OF_RESULT])
        logger.info(f"Собрал результаты! Количество")
        self.all_pages[page_id][self.COUNT_OF_RESULT] += len(parsed_data)

        await self.smooth_scroll_wheel(page_id=page_id, distance=2000)
        await asyncio.sleep(uniform(0.2, 0.5))

        await page().wait_for_selector(".sb_pagN", timeout=5000)

        loc_pag_next = page().locator(".sb_pagN.sb_inactP")
        if await loc_pag_next.count() > 0:
            logger.info("Кнопка пагинации не активна! Иду дальше...")
            return True

        await page().click(".sb_pagN", timeout=2000)
        logger.info("Кликнул на пагинацию вперед")

    async def get_lambda_c_page(self, page_id: int):
        return lambda: self.all_pages[page_id][self.C_PAGE]

    async def get_lambda_c_url(self, page_id: int):
        return lambda: self.all_pages[page_id][self.C_URL]

    async def get_lambda_c_query(self, page_id: int):
        return lambda: self.all_pages[page_id][self.C_QUERY]

    async def get_lambda_count_of_result(self, page_id: int):
        return lambda: self.all_pages[page_id][self.COUNT_OF_RESULT]

    async def get_lambda_count_of_page(self, page_id: int):
        return lambda: self.all_pages[page_id][self.COUNT_OF_PAGE]
