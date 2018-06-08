# -*- coding: utf-8 -*-
import re

from scrapy import Spider, Request
from Qiongyou.items import TopicItem, ReplyItem


class QiongyouSpider(Spider):
    name = 'qiongyou'
    allowed_domains = ['bbs.qyer.com']
    start_url_main = 'http://bbs.qyer.com/forum-53-{}.html'
    start_url_partners = 'http://bbs.qyer.com/forum-53-3-{}.html'
    js_url = 'http://bbs.qyer.com/detail/content/p/{}.json?ajaxID=5aa7a6618776329315f6d14c&time_sta='
    start_page = '1'

    def start_requests(self):
        yield Request(url=self.start_url_main.format(self.start_page), callback=self.parse_index_main)
        yield Request(url=self.start_url_partners.format(self.start_page), callback=self.parse_index_partners)

    def parse_index_main(self, response):
        page = int(response.css('a.ui_page_item::text').extract()[5][3:])
        for i in range(1, page+1):
            yield Request(url=self.start_url_main.format(i), callback=self.parse_topics, dont_filter=True)

    def parse_index_partners(self, response):
        page = int(response.css('a.ui_page_item::text').extract()[5][3:])
        for i in range(1, page+1):
            yield Request(url=self.start_url_partners.format(i), callback=self.parse_topics, dont_filter=True)

    def parse_topics(self, response):
        table = response.css('.cntdl.clearfix')

        for row in table:
            item = TopicItem()
            type = row.css('a.type::text').extract_first().strip()
            title = row.css('a.txt::text').extract_first().strip()
            link = 'http:' + row.css('a.txt::attr(href)').extract_first()
            writer = row.css('dd.data a::text').extract_first().strip()
            post_time = row.css('span.date::text').extract_first()
            profile_link = 'http:' + row.css('dd.data a::attr(href)').extract_first()
            departure_time = ''
            return_time = ''
            destination = ''
            views = row.css('.poi::text').extract_first()
            replies = row.css('.reply::text').extract_first()
            likes = row.css('.like::text').extract_first()
            if row.css('.x-gowith-listinfo .xltime'):
                departure_time = row.css('.x-gowith-listinfo .xltime::text').extract()[0].strip()
                return_time = row.css('.x-gowith-listinfo .xltime::text').extract()[1].strip()
            if row.css('.gowith-dest'):
                destination = row.css('.gowith-dest::text').extract_first().replace(' ', '').strip()
            item['type'] = type
            item['title'] = title
            item['link'] = link
            item['writer'] = writer
            item['post_time'] = post_time
            item['profile_link'] = profile_link
            item['depature_time'] = departure_time
            item['return_time'] = return_time
            item['destination'] = destination
            item['views'] = views
            item['replies'] = replies
            item['likes'] = likes
            yield item
            yield Request(url=link, callback=self.parse_replies)

    def parse_replies(self, response):
        table = response.css('.detail_inner .floor_item.com_pad')
        for row in table:
            item = ReplyItem()
            post_id = row.css('::attr(id)').extract_first()[10:]
            profile_link = 'http:' + row.css('.user_info .center_top a::attr(href)').extract_first()
            name = row.css('.user_info .center_top a::text').extract_first().strip()
            post_time = row.css('.user_info .center_bottom .time::text').extract_first().strip()[4:]
            content = ''
            pattern = re.compile('.*?thread-(.*?)-.*?', re.S)
            topic_id = re.findall(pattern, response.url)[0]
            item['post_id'] = post_id
            item['profile_link'] = profile_link
            item['name'] = name
            item['post_time'] = post_time
            item['content'] = content
            item['topic_id'] = topic_id
            item['update'] = False
            yield item
            yield Request(url=self.js_url.format(post_id), callback=self.parse_js)

    def parse_js(self, response):
        item = ReplyItem()
        item['update'] = True
        pattern = re.compile('.*?/p/(.*?)\.json.*?', re.S)
        item['post_id'] = re.findall(pattern, response.url)[0]
        pattern = re.compile('.*?"content":"(.*?)","first"', re.S)
        item['content'] = re.sub('<.*?>', '', re.findall(pattern, response.text)[0].strip())
        yield item



