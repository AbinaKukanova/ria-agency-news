import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd
from nltk.tokenize import sent_tokenize
from tqdm import tqdm
from tqdm import trange
import nltk
from urllib3 import disable_warnings, exceptions

disable_warnings(exceptions.InsecureRequestWarning)

# nltk.download('punkt')

start_time = datetime.datetime.now()

HEADERS = {
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36 OPR/40.0.2308.81',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'DNT': '1',
    'Accept-Encoding': 'gzip, deflate, lzma, sdch',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4'
}

MAX_RETRIES = 20


class RiaAgencyParser:

    def __init__(self):
        self.session = self.create_session()

    def create_session(self):
        session = requests.Session()
        retries = Retry(total=MAX_RETRIES, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('https://', adapter)
        session.mount('https://', adapter)
        return session

    def get_html_pages(self, article_url: str, start_date: str, end_date: str):
        start_date = datetime.datetime.strptime(start_date, "%Y.%m.%d").date()
        end_date = datetime.datetime.strptime(end_date, "%Y.%m.%d").date()
        current_date = start_date
        pages = []
        total_days = (end_date - start_date).days + 1
        with tqdm(total=total_days, desc="Processing Archive by Dates") as pbar:
            while current_date <= end_date:
                date_str = current_date.strftime("%Y%m%d")
                news_page_url = f"{article_url}/{date_str}"
                response = self.session.get(news_page_url, headers=HEADERS, verify=False)
                soup = BeautifulSoup(response.text, 'html.parser')
                links = soup.find_all("a", {"class": "list-item__title"})
                for href in links:
                    pages.append(href.get("href"))

                time.sleep(5)
                pbar.update(1)
                current_date += datetime.timedelta(days=1)

        return pages

    def extract_info_from_pages(self, articles: list):
        titles = []
        dates = []
        texts = []
        for i in (pbar := trange(len(articles))):
            pbar.set_description("Parsing html pages")
            response = self.session.get(url=articles[i], headers=HEADERS, verify=False)
            soup = BeautifulSoup(response.text, 'html.parser')
            if soup.find(["div", "h1"], attrs={"class": "article__title"}):
                title = soup.find(["div", "h1"], attrs={"class": "article__title"}).text
            else:
                title = None

            if soup.findAll("div", class_="article__text"):
                text = ' '.join([i.text for i in soup.findAll("div", class_="article__text")])
                sents = sent_tokenize(text)
                text = ' '.join(sents[1:])
            else:
                text = None

            if soup.find("div", class_="article__info-date").find("a"):
                date_element = soup.find("div", class_="article__info-date").find("a")
                date_text = date_element.text.strip()
                date_time = date_text.split()
                date_part = date_time[1]
            else:
                date_part = None

            titles.append(title)
            dates.append(date_part)
            texts.append(text)
            time.sleep(5)

        return titles, dates, texts

    def save_to_csv(self, titles: list, dates: list, texts: list,
                    file_name: str):
        ria_news_dataset = pd.DataFrame(list(zip(titles, dates, texts)), columns=['title', 'date', 'text'])
        ria_news_dataset.to_csv(f"{file_name}.csv", index=False, encoding='utf-8')

    def main(self):
        start_date = "2024.05.01"
        end_date = "2024.05.02"

        url = "https://ria.ru/"
        file_name = 'ria-2016-2019'
        parser = RiaAgencyParser()

        print('Starting the process...')
        articles = parser.get_html_pages(url, start_date, end_date)
        print(len(articles))
        print('The links are downloaded')
        print('Extracting info from each article page...')
        titles, dates, texts = parser.extract_info_from_pages(articles)
        print(len(titles))
        print(len(dates))
        print(len(texts))
        print('The information has been saved')
        saving_results = parser.save_to_csv(titles, dates, texts, file_name)
        print('The results are saved.')
        print()


if __name__ == "__main__":
    ria = RiaAgencyParser()
    ria.main()

end_time = datetime.datetime.now()
print('Duration: {}'.format(end_time - start_time))