# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Field, Item


class TopicItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    type = Field()
    title = Field()
    link = Field()
    writer = Field()
    post_time = Field()
    profile_link = Field()
    depature_time = Field()
    return_time = Field()
    destination = Field()
    views = Field()
    replies = Field()
    likes = Field()

    def istopic(self):
        return True


class ReplyItem(Item):
    post_id = Field()
    profile_link = Field()
    name = Field()
    post_time = Field()
    content = Field()
    topic_id = Field()
    update = Field()

    def istopic(self):
        return False
