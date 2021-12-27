"""Spider to extract information from a /author/show page"""
import logging

import scrapy
from scrapy import Request
from scrapy.http import XmlResponse
from scrapy.spiders.sitemap import iterloc
from scrapy.utils.gz import gzip_magic_number, gunzip
from scrapy.utils.sitemap import Sitemap

from GoodreadsScraper.items import UserProfileItem

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class UserIdSpider(scrapy.Spider):
    name = "user_id"
    start_urls = (
        'https://www.goodreads.com/siteindex.user.xml',
    )

    def parse(self, response):
        body = self._get_sitemap_body(response)
        sitemap = Sitemap(body)
        if sitemap.type == 'sitemapindex':
            for loc in iterloc(sitemap, False):
                yield Request(loc, callback=self.parse)
        else:
            for site_link in sitemap:
                url = site_link['loc']
                yield self.parse_user_profile(url)

    @staticmethod
    def parse_user_profile(url):
        return UserProfileItem({"profile_url": url})

    def _get_sitemap_body(self, response):
        """Return the sitemap body contained in the given response,
        or None if the response is not a sitemap.
        """
        if isinstance(response, XmlResponse):
            return response.body
        elif gzip_magic_number(response):
            return gunzip(response.body)
        # actual gzipped sitemap files are decompressed above ;
        # if we are here (response body is not gzipped)
        # and have a response for .xml.gz,
        # it usually means that it was already gunzipped
        # by HttpCompression middleware,
        # the HTTP response being sent with "Content-Encoding: gzip"
        # without actually being a .xml.gz file in the first place,
        # merely XML gzip-compressed on the fly,
        # in other word, here, we have plain XML
        elif response.url.endswith('.xml') or response.url.endswith('.xml.gz'):
            return response.body


def iterloc(iterable, alt=False):
    for d in iterable:
        yield d['loc']

        # Also consider alternate URLs (xhtml:link rel="alternate")
        if alt and 'alternate' in d:
            for l in d['alternate']:
                yield l
