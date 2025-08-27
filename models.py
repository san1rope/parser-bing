from typing import Any

from playwright.async_api import Page
from pydantic import BaseModel


class ProxyData(BaseModel):
    id: int
    host: str
    port: str
    username: str = None
    password: str = None
    available: bool = True

    def __str__(self):
        return f"{self.host}:{self.port}:{self.username}:{self.password}"


class QueueMessage(BaseModel):
    msg_type: str
    data: Any = None


class SearchResult(BaseModel):
    query: str
    page: str
    position: str
    title: str
    url: str
    snippet: str
    lang: str
    country: str
    mkt: str
    user_agent: str
    proxy: str

    def to_list(self):
        return [
            self.query, self.page, self.position, self.title, self.url, self.snippet, self.lang, self.country,
            self.mkt, self.user_agent, self.proxy
        ]
