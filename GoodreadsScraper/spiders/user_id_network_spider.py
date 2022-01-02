"""Spider to extract information from a /author/show page"""
import logging
import re
from urllib.parse import urljoin

import scrapy
from scrapy import Request

from GoodreadsScraper.items import UserProfileItem

logger = logging.getLogger(__name__)

BOOK_REGEX = re.compile(r".*\b(\d+)\sbooks")
BOOKS_FOLLOW_THRESHOLD = 50
GOODREADS_URL_PREFIX = "https://www.goodreads.com"


class UserIdNetworkSpider(scrapy.Spider):
    name = "user_id_network"
    custom_settings = {'CLOSESPIDER_ITEMCOUNT': 100_000,
                       'ITEM_PIPELINES': {'GoodreadsScraper.pipelines.GcpTaskQueuePipeline': 400}}
    start_urls = [
        'https://www.goodreads.com/user/show/24697113-david-fennessey',
    ]

    def parse(self, response):
        # Don't scrape author pages, it's too annoying to get their read list
        if not response.url.startswith("https://www.goodreads.com/user/show/"):
            logger.debug(f"skipping page {response.url}")
            return

        yield self.parse_user_profile(response.url)
        for friend_block in response.xpath('//div[@class="left"]'):
            friend_url = urljoin(GOODREADS_URL_PREFIX, friend_block.xpath('div[@class="friendName"]//a/@href').get())
            friend_count = self.extract_friend_count(friend_block)
            if friend_count > BOOKS_FOLLOW_THRESHOLD:
                yield Request(friend_url, callback=self.parse)

    @staticmethod
    def extract_friend_count(selector_block):
        friend_count = 0
        for div_text in selector_block.xpath('text()').getall():
            regex_results = re.findall(BOOK_REGEX, div_text)
            if len(regex_results) > 0:
                friend_count = int(regex_results[0])
        return friend_count

    @staticmethod
    def parse_user_profile(url):
        return UserProfileItem({"profile_url": url})
