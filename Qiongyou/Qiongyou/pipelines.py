# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import logging

import pymongo


class QiongyouPipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipeline(object):
    logger = logging.getLogger(__name__)

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def process_item(self, item, spider):
        if item.istopic():
            if self.db['topics'].update({'link': item['link']}, {'$set': item}, True):
                self.logger.debug('Saved to Mongo {}'.format(item['title']))
            else:
                self.logger.debug('Saved to Mongo failed {}'.format(item['link']))
            return item
        elif not item['update']:
            if self.db['replies'].update({'post_id': item['post_id']}, {'$set': item}, True):
                self.logger.debug('Saved to Mongo {}'.format(item['post_id']))
            else:
                self.logger.debug('Saved to Mongo failed {}'.format(item['link']))
            return item
        else:
            if self.db['replies'].update({'post_id': item['post_id']}, {'$set': {'update': True, 'content': item['content']}}, True):
                self.logger.debug('Saved to Mongo {}'.format(item['post_id']))
            else:
                self.logger.debug('Saved to Mongo failed {}'.format(item['link']))
            return item

    def close_spider(self, spider):
        self.client.close()
