"""Spider to extract information from a /author/show page"""
import logging
import re

import scrapy
from scrapy import Request
from scrapy.http import XmlResponse
from scrapy.spiders.sitemap import iterloc
from scrapy.utils.gz import gzip_magic_number, gunzip
from scrapy.utils.sitemap import Sitemap

from GoodreadsScraper.items import UserProfileItem

logger = logging.getLogger(__name__)
USER_ID_NAME_EXTRACTOR = re.compile(".*/user/show/(.*$)")


class UserReviewsSpider(scrapy.Spider):
    name = "user_reviews"

    def __init__(self, url_list=[]):
        super().__init__()
        url_list = [
            "https://www.goodreads.com/user/show/3114744-david-basile",
            "https://www.goodreads.com/user/show/10551948-david-vandyke",
            "https://www.goodreads.com/user/show/326912-david-van-den-bossche",
            "https://www.goodreads.com/user/show/21512610-titus-david"
        ]
        self.start_urls = self.convert_profiles_to_reviews_pages(url_list)

    def parse(self, response):
        a = 5

    @staticmethod
    def convert_profiles_to_reviews_pages(urls):
        ids = []
        for url in urls:
            user_ids_and_names = re.findall(USER_ID_NAME_EXTRACTOR, url)
            if len(user_ids_and_names) > 0:
                ids.append(user_ids_and_names[0])
            else:
                logger.info(f"Could not parse user profile at url {url}")

        return [f"https://www.goodreads.com/review/list/{user_id}?shelf=read" for user_id in ids]
