import argparse
import asyncio
import csv
import logging
from concurrent.futures import ProcessPoolExecutor
import datetime
from multiprocessing import cpu_count
from nltk.tokenize import sent_tokenize
import aiohttp
from bs4 import BeautifulSoup
import ssl
import certifi


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s @ %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)
logger = logging.getLogger(name="RiaAgencyParser")


class RiaAgencyParser:
    # lxml is much faster but error prone
    default_parser = "html.parser"

    def __init__(self, *, max_workers: int, outfile_name: str, from_date: str, to_date: str):
        self._endpoint = "https://ria.ru/"

        self._sess = None
        self._connector = None

        self._executor = ProcessPoolExecutor(max_workers=max_workers)

        self._outfile_name = outfile_name
        self._outfile = None
        self._csv_writer = None
        self.timeouts = aiohttp.ClientTimeout(total=60, connect=60)
        self._sslcontext = ssl.create_default_context(cafile=certifi.where())

        self._n_downloaded = 0
        self._from_date = datetime.datetime.strptime(from_date, "%Y.%m.%d").date()
        self._to_date = datetime.datetime.strptime(to_date, "%Y.%m.%d").date()

    @property
    def dates_countdown(self):
        date_start = self._from_date
        date_end = self._to_date

        while date_start <= date_end:
            yield date_start.strftime("%Y%m%d")
            date_start += datetime.timedelta(days=1)

    @property
    def writer(self):
        if self._csv_writer is None:
            self._outfile = open(self._outfile_name, "w", 1, encoding='utf-8')
            self._csv_writer = csv.DictWriter(
                self._outfile, fieldnames=["url", "title", 'date', "text"]
            )
            self._csv_writer.writeheader()

        return self._csv_writer

    async def fetch(self, url: str):
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(
                use_dns_cache=True, ttl_dns_cache=60 * 60, limit=1024),
                timeout=self.timeouts) as session:
            async with session.get(url, ssl=self._sslcontext) as response:
                return await response.text()

    @staticmethod
    def parse_article_html(html: str):
        doc_tree = BeautifulSoup(html, "html.parser")
        if doc_tree.find(["div", "h1"], attrs={"class": "article__title"}):
            title = doc_tree.find(["div", "h1"], attrs={"class": "article__title"}).text
        else:
            title = None

        if doc_tree.findAll("div", class_="article__text"):
            text = ' '.join([i.text for i in doc_tree.findAll("div", class_="article__text")])
            sents = sent_tokenize(text)
            text = ' '.join(sents[1:])
        else:
            text = None

        if doc_tree.find("div", class_="article__info-date").find("a"):
            date_element = doc_tree.find("div", class_="article__info-date").find("a")
            date_text = date_element.text.strip()
            date_time = date_text.split()
            date_part = date_time[1]
        else:
            date_part = None

        return {"title": title, "date": date_part, "text": text}

    @staticmethod
    def _extract_urls_from_html(html: str):
        doc_tree = BeautifulSoup(html, "html.parser")
        news_list = doc_tree.find_all("a", {"class": "list-item__title"})
        return tuple(news.get("href") for news in news_list)

    async def _fetch_all_news_on_page(self, initial_html: str):
        news_urls = []
        html = initial_html
        page_news_urls = await asyncio.to_thread(self._extract_urls_from_html, html)
        news_urls.extend(page_news_urls)

        tasks = [asyncio.create_task(self.fetch(url)) for url in set(news_urls)]
        fetched_raw_news = {}
        for i, task in enumerate(tasks):
            try:
                fetch_res = await task
            except aiohttp.ClientResponseError as exc:
                logger.error(f"Cannot fetch {exc.request_info.url}: {exc}")
            except asyncio.TimeoutError:
                logger.exception("Cannot fetch. Timeout")
            else:
                fetched_raw_news[news_urls[i]] = fetch_res

        parse_tasks = {url: asyncio.to_thread(self.parse_article_html, html) for url, html in fetched_raw_news.items()}
        parsed_news = []

        for url, task in parse_tasks.items():
            try:
                parse_res = await task
            except Exception as e:
                logger.exception(f"Cannot parse {url}: {e}")
            else:
                parse_res["url"] = url
                parsed_news.append(parse_res)

        if parsed_news:
            self.writer.writerows(parsed_news)
            self._n_downloaded += len(parsed_news)

        return len(parsed_news)

    async def _producer(self):
        for date in self.dates_countdown:
            news_page_url = f"{self._endpoint}/{date}"

            try:
                html = await asyncio.create_task(self.fetch(news_page_url))
            except aiohttp.ClientResponseError:
                logger.exception(f"Cannot fetch {news_page_url}")
            except aiohttp.ClientConnectionError:
                logger.exception(f"Cannot fetch {news_page_url}")
            except BaseException as e:
                logger.info(f"Cannot fetch {news_page_url}: {e}")
            else:
                n_proccessed_news = await self._fetch_all_news_on_page(html)

                if n_proccessed_news == 0:
                    logger.info(f"News not found at {news_page_url}.")

                logger.info(
                    f"{news_page_url} processed ({n_proccessed_news} news). "
                    f"{self._n_downloaded} news saved totally."
                )

    async def run(self):
        await self._producer()


def main():
    parser = argparse.ArgumentParser(description="Downloads news from Lenta.Ru")

    parser.add_argument(
        "--outfile", default="ria-agency-news.csv", help="name of result file"
    )

    parser.add_argument(
        "--cpu-workers", default=cpu_count(), type=int, help="number of workers"
    )

    parser.add_argument(
        "--from-date",
        default="2013.01.01",
        type=str,
        help="download news from this date. Example: 2024.05.01",
    )
    parser.add_argument(
        "--to-date",
        default="2023.12.32",
        type=str,
        help="download news from this date. Example: 2024.05.03",
    )
    args = parser.parse_args()

    parser = RiaAgencyParser(
        max_workers=args.cpu_workers,
        outfile_name=args.outfile,
        from_date=args.from_date,
        to_date=args.to_date
    )

    asyncio.run(parser.run())


if __name__ == "__main__":
    main()