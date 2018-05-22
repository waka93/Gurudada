import requests
from requests.exceptions import ConnectionError
from bs4 import BeautifulSoup
from multiprocessing import Pool
import re
import pymongo
import time
import random
from config import *


proxy = None


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


def get_html(url, count=1):
    global proxy
    print('Crawling Page: ', url)
    if count >= MAX_COUNT:
        print('Server no response')
        print('Spider sleeping ..')
        time.sleep(SLEEP_CYCLE*random.random())
        return get_html(url)
    try:
        if proxy:
            print('Using Proxy', proxy)
            proxies = {
                'http': 'http://' + proxy
            }
            response = requests.get(url, allow_redirects=False, headers=HEADERS, proxies=proxies)
        else:
            proxy = get_proxy()
            print('Not Using Proxy. Trying to Get New Proxy.')
            return get_html(url)
        if response.status_code == 200:
            return response.text
        else:
            proxy = get_proxy()
            if proxy:
                print('Response Status', response.status_code)
                print('Try Again')
                return get_html(url)
            else:
                print('Response Status', response.status_code)
                print('Getting Proxy Failed')
                print('Spider sleeping ...')
                time.sleep(SLEEP_CYCLE*random.random())
                return get_html(url)
    except ConnectionError:
        print('Connection Error. Try again')
        proxy = get_proxy()
        count += 1
        return get_html(url, count)


def get_index_page(page):
    url = BASE_URL.format(str(page))
    html = get_html(url)
    if html:
        return html
    else:
        return get_index_page(page)


def parse_index(html):
    print('Parsing Page ...')
    soup = BeautifulSoup(html, 'lxml')
    table = soup.select('.cntdl.clearfix')
    for row in table:
        info = {
            'type': row.select('a')[0].string.strip(),
            'title': row.select('a')[1].string.strip(),
            'link': 'https:' + row.select('a')[1]['href'],
            'writer': row.select('a')[2].string.strip(),
            'profile_link': 'https:' + row.select('a')[2]['href'],
            'depature_time': '',
            'return_time': '',
            'destination': '',
            'views': '',
            'replies': ''
        }
        if row.select('.x-gowith-listinfo .xltime'):
            info['depature_time'] = row.select('.x-gowith-listinfo .xltime')[0].string.strip()
            info['return_time'] = row.select('.x-gowith-listinfo .xltime')[1].string.strip()
        if row.select('.gowith-dest'):
            info['destination'] = row.select('.gowith-dest')[0].string.replace(' ','').strip()
        views = str(row.select('.poi')[0])
        pattern = re.compile('.*?</i>(.*?)</span>', re.S)
        info['views'] = re.findall(pattern, views)[0]
        replies = str(row.select('.reply')[0])
        pattern = re.compile('.*?</i>(.*?)</span>', re.S)
        info['replies'] = re.findall(pattern, replies)[0]
        yield info


def save_to_mongo(data):
    client = pymongo.MongoClient('localhost')
    db_topics = client['Qiongyou_American']
    if db_topics['topics'].update({'link': data['link']}, {'$set': data}, True):
        print('Saved to Mongo', data['title'])
    else:
        print('Saved to Mongo failed', data['link'])


def main(page):
    for item in parse_index(get_index_page(page)):
        save_to_mongo(item)


if __name__ == '__main__':
    pool = Pool()
    pool.map(main, range(1, 101))

