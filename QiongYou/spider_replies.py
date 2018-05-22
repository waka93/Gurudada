from save_in_json import read_data
import requests
from requests.exceptions import ConnectionError, Timeout
from bs4 import BeautifulSoup
import re
from config import *
import time
import random
import pymongo
from multiprocessing import Pool

proxy = None


class RepliesSpider(object):
    def __init__(self, data):
        self._type = data['type']
        self._link = data['link']
        self._all_links = self.max_page()

    @staticmethod
    def get_proxy():
        try:
            response = requests.get('http://127.0.0.1:5000/get')
            if response.status_code == 200:
                if response.text:
                    return response.text
                else:
                    print('Proxy Pool Empty')
                    return None
            else:
                print('Getting Proxy Error')
                time.sleep(POOL_REFRESHING_TIME)
                return None
        except ConnectionError:
            print('Cannot Connect to Proxy Pool')
            return None

    @staticmethod
    def get_html(url, count=1):
        global proxy
        print('Crawling Page: ', url)
        if count >= MAX_COUNT:
            print('Server no response')
            print('Spider sleeping ..')
            time.sleep(SLEEP_CYCLE * random.random())
            return RepliesSpider.get_html(url)
        try:
            if proxy:
                print('Using Proxy', proxy)
                proxies = {
                    'http': 'http://' + proxy
                }
                try:
                    response = requests.get(url, allow_redirects=False, headers=HEADERS, proxies=proxies, timeout=TIMEOUT)
                except Timeout:
                    print('Connection Timeout. Try Another Proxy')
                    proxy = RepliesSpider.get_proxy()
                    return RepliesSpider.get_html(url)
            else:
                proxy = RepliesSpider.get_proxy()
                print('Not Using Proxy. Trying to Get New Proxy.')
                return RepliesSpider.get_html(url)
            if response.status_code == 200:
                return response.content.decode('utf-8')
            else:
                proxy = RepliesSpider.get_proxy()
                if proxy:
                    print('Response Status', response.status_code)
                    print('Try Again')
                    return RepliesSpider.get_html(url)
                else:
                    print('Response Status', response.status_code)
                    print('Getting Proxy Failed')
                    print('Spider sleeping ...')
                    time.sleep(SLEEP_CYCLE * random.random())
                    return RepliesSpider.get_html(url)
        except ConnectionError:
            print('Connection Error. Try again')
            proxy = RepliesSpider.get_proxy()
            count += 1
            return RepliesSpider.get_html(url, count)

    def get_index_page(self):
        html = RepliesSpider.get_html(self._link)
        if html:
            return html
        else:
            return self.get_index_page()

    def max_page(self):
        print('Locating Paginator ...')
        html = self.get_index_page()
        soup = BeautifulSoup(html, 'lxml')
        if soup.select('.qui-pager-item'):
            pattern = re.compile('.*?(\d+)</a>', re.S)
            max_page = re.findall(pattern, str(soup.select('.qui-pager-item')[-2]))[0]
            self._all_links = [self._link[:-6] + '{}.html'.format(str(page)) for page in range(1, int(max_page))]
            print('Paginator Located')
        else:
            self._all_links = [self._link]
            print('Paginator Not Found. Only One Page.')
        return self._all_links

    @staticmethod
    def parse_single_page(url):
        html = RepliesSpider.get_html(url)
        print('Parsing Page ...', url)
        soup = BeautifulSoup(html, 'lxml')
        table = soup.select('.detail_inner')[0].select('.floor_item.com_pad')
        for row in table:
            info = {
                'post_id': row['id'][10:],
                'profile': 'http:' + row.select('.user_info .center_top')[0].select('a')[0]['href'],
                'name': row.select('.user_info .center_top')[0].select('a')[0].string.strip(),
                'time': row.select('.user_info .center_bottom')[0].select('.time')[0].string.strip()[4:],
                'content': '',
                'topic_id': ''
            }
            json_url = JS_URL.format(info['post_id'])
            print('Getting Replies By Id', info['post_id'])
            json_content = RepliesSpider.get_html(json_url)
            pattern = re.compile('.*?"content":"(.*?)","first"', re.S)
            info['content'] = re.findall(pattern, json_content)[0]
            pattern = re.compile('"tid":"(\d+)"')
            info['topic_id'] = re.findall(pattern, json_content)[0]
            yield info

    def parse_all_pages(self):
        client = MongoClient()
        for link in self._all_links:
            for item in RepliesSpider.parse_single_page(link):
                client.save_to_mongo(item)


class MongoClient(object):
    def __init__(self,):
        self._client = pymongo.MongoClient('localhost')
        self._db_replies = self._client['Qiongyou_American']

    def save_to_mongo(self, data):
        if self._db_replies['replies'].update({'post_id': data['post_id']}, {'$set': data}, True):
            print('Saved to Mongo', data['post_id'])
        else:
            print('Saved to Mongo failed', data['post_id'])

    @staticmethod
    def read_data():
        client = pymongo.MongoClient('localhost')
        db = client['Qiongyou_American']
        table = db.replies.find()
        replies = []
        for row in table:
            replies.append(row)
        return replies


def main(data):
    spider = RepliesSpider(data)
    spider.parse_all_pages()


if __name__ == '__main__':
    _, raw_data = read_data()
    pool = Pool()
    pool.map(main, raw_data)


