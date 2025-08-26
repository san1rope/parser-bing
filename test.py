import base64
import re
import asyncio
import logging
from typing import List
from urllib.parse import urlencode
from urllib.parse import urlparse, parse_qs, unquote, urljoin

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from selectolax.parser import HTMLParser

logger = logging.getLogger(__name__)
BING = "https://www.bing.com"


def _clean_bing_href(href: str) -> str | None:
    """Возвращает «чистый» URL, снимая редиректы Bing; либо сам href, либо None."""
    if not href or href.startswith("javascript:") or href == "#":
        return None
    # абсолютный путь
    if href.startswith("/"):
        href = urljoin(BING, href)
    p = urlparse(href)
    # если уже не bing — это чистая ссылка
    if p.netloc and not p.netloc.endswith("bing.com") and p.netloc != "go.microsoft.com":
        return href

    qs = parse_qs(p.query)
    for key in ("u", "r", "url", "target", "mediaurl"):
        val = qs.get(key) or qs.get(key.upper())
        if val and val[0]:
            url = unquote(val[0])
            # иногда двойное кодирование
            if "%2F" in url or "%3A" in url:
                url = unquote(url)
            return url
    return None  # не удалось извлечь цель (например, внутренняя ссылка bing)


def _text(el) -> str:
    return re.sub(r"\s+", " ", el.text(strip=True)) if el else ""


async def main(max_snippet_len: int = 300):
    logging.basicConfig(level=logging.INFO,
                        format=u'%(filename)s:%(lineno)d #%(levelname)-8s [%(asctime)s] - %(name)s - %(message)s')

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            proxy={
                "server": "http://s-29774.sp6.ovh:11001",
                "username": "qV6VPX3gU0_0",
                "password": "mypassword"
            }
        )

        context = await browser.new_context()
        page = await context.new_page()
        page2 = await context.new_page()

        input()
        await Stealth().apply_stealth_async(page)

        params = {
            "q": "html5",
            "qs": "n",
            "form": "QBRE",
            "sp": "-1",
            "ghc": "1",
            "lq": "0",
            "pq": "html",
            "sc": "12-4",
            "sk": "",
            "first": "5",
            "count": "8"
        }
        domain = "https://www.bing.com/search"
        full_url = f"{domain}?{urlencode(params)}"
        print(f"full_url = {full_url}")

        await page.goto(full_url)

        input("enter: ")

        tree = HTMLParser(await page.content())

        # # Links
        # links = []
        # for a in html.css("li.b_algo h2 a"):
        #     href = a.attrs.get("href")
        #     if href and href not in links:
        #         links.append(href)
        #
        # # Snippet

        out = []

        container = tree.css_first("#b_results") or tree
        items = container.css("li.b_algo")  # органические результаты

        for li in items:
            a = li.css_first("h2 a") or li.css_first("h3 a") or li.css_first(".b_title a")
            if not a:
                continue

            raw_href = a.attrs.get("href", "")
            url = _clean_bing_href(raw_href) or (raw_href if raw_href else None)
            if not url:
                continue

            title = _text(a)
            if not title:
                continue

            snippet = ""
            for sel in (".b_caption p", ".b_snippet", ".b_desc", ".b_caption", "p"):
                el = li.css_first(sel)
                snippet = _text(el)
                if snippet and len(snippet) > 10:
                    break

            if max_snippet_len and snippet:
                snippet = snippet[:max_snippet_len].rstrip()

            url += "=" * (-len(url[2:]) % 4)
            out.append({"title": title, "url": url, "snippet": snippet})

        print(f"len of `out` = {len(out)}")
        print(*out, sep="\n")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
