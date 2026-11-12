from typing import TYPE_CHECKING

from curl_cffi.requests import get
from curl_cffi.requests.exceptions import RequestException

from ..module import ERROR, logging, retry, sleep_time
from ..translation import _

if TYPE_CHECKING:
    from ..module import Manager

__all__ = ["Html"]


class Html:
    def __init__(
        self,
        manager: "Manager",
    ):
        self.manager = manager
        self.print = manager.print
        self.retry = manager.retry
        self.client = manager.request_client
        self.timeout = manager.timeout
        self.proxy = manager.proxy
        self.impersonate = manager.impersonate
        # 最近一次请求的最终跳转地址（小红书对不可访问笔记也返回 200，
        # 靠它才能区分「内容没了」与「token 失效」）
        self.last_final_url: str = ""
        # 本次运行里「获取数据失败」的待落盘清单
        self.failures: list = []

    @retry
    async def request_url(
        self,
        url: str,
        content=True,
        cookie: str | None = None,
        proxy: str | None = None,
        **kwargs,
    ) -> str:
        if not url.startswith("http"):
            url = f"https://{url}"
        headers = self.manager.get_headers(url)
        try:
            if cookie:
                response = self.__request_url_with_cookie(
                    url,
                    headers,
                    cookie,
                    proxy=proxy,
                    **kwargs,
                )
            else:
                response = await self.__request_url_get(
                    url,
                    headers,
                    proxy=proxy,
                    **kwargs,
                )
            await sleep_time()
            response.raise_for_status()
            self.last_final_url = str(getattr(response, "url", "") or "")
            return response.text if content else self.last_final_url
        except RequestException as error:
            self.last_final_url = ""
            logging(
                self.print,
                _("网络异常，{0} 请求失败: {1}").format(url, repr(error)),
                ERROR,
            )
            return ""

    @staticmethod
    def format_url(url: str) -> str:
        return bytes(url, "utf-8").decode("unicode_escape")

    def __request_url_with_cookie(
            self,
            url: str,
            headers: dict,
            cookie: str,
            proxy: str | None = None,
            **kwargs,
    ):
        return get(
            url,
            headers=headers,
            cookies=self.manager.cookie_str_to_dict(cookie),
            timeout=self.timeout,
            verify=False,
            allow_redirects=True,
            proxy=self.proxy if proxy is None else proxy,
            impersonate=self.impersonate,
            **kwargs,
        )

    async def __request_url_get(
        self,
        url: str,
        headers: dict,
        proxy: str | None = None,
        **kwargs,
    ):
        return await self.client.get(
            url,
            headers=headers,
            proxy=self.proxy if proxy is None else proxy,
            **kwargs,
        )
