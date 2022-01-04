"""Spider to extract information from a /book/show type page on Goodreads"""
import re

import scrapy
from urllib.parse import urlsplit

from ..items import BookItem, BookLoader

SIMILAR_EDITIONS_ISBN_REGEX = re.compile(r"editionInfo\\'>\\nisbn:\s(\d+)\\n<\\")
SIMILAR_EDITIONS_ISBN_13_REGEX = re.compile(r"editionInfo\\'>\\nisbn13:\s(\d+)\\n<\\")
SIMILAR_EDITIONS_ASIN_REGEX = re.compile(r"editionInfo\\'>\\nasin:\s([A-Za-z0-9]+)\\n<\\")


class BookSpider(scrapy.Spider):
    """Extract information from a /book/show type page on Goodreads"""
    name = "book"

    def __init__(self, books=None):
        super().__init__()
        self.start_urls = books.split(",")
        # For debugging
        #self.start_urls = ["https://www.goodreads.com/book/show/5907.The_Hobbit_or_There_and_Back_Again"]

    def parse(self, response):
        loader = BookLoader(BookItem(), response=response)

        # I use relative paths because that's what's in the reviews
        loader.add_value('url', urlsplit(response.request.url).path)

        loader.add_css("title", "#bookTitle::text")
        loader.add_css("author", "a.authorName>span::text")
        loader.add_css("author_url", 'a.authorName::attr(href)')

        loader.add_css("num_ratings", "[itemprop=ratingCount]::attr(content)")
        loader.add_css("num_reviews", "[itemprop=reviewCount]::attr(content)")
        loader.add_css("avg_rating", "span[itemprop=ratingValue]::text")
        loader.add_css("num_pages", "span[itemprop=numberOfPages]::text")

        loader.add_css("language", "div[itemprop=inLanguage]::text")
        loader.add_css('publish_date', 'div.row::text')
        loader.add_css('publish_date', 'nobr.greyText::text')

        loader.add_css('original_publish_year', 'nobr.greyText::text')

        loader.add_css("genres", 'div.left>a.bookPageGenreLink[href*="/genres/"]::text')
        loader.add_css('series', 'div.infoBoxRowItem>a[href*="/series/"]::text')

        loader.add_css('asin', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('asin', 'script::text', re=SIMILAR_EDITIONS_ASIN_REGEX)
        loader.add_css('isbn', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('isbn', 'span[itemprop=isbn]::text')
        loader.add_css('isbn', 'div.infoBoxRowItem::text')
        loader.add_css('isbn', 'div.infoBoxRowItem::text')
        loader.add_css('isbn', 'script::text', re=SIMILAR_EDITIONS_ISBN_REGEX)
        loader.add_css('isbn13', 'div.infoBoxRowItem[itemprop=isbn]::text')
        loader.add_css('isbn13', 'span[itemprop=isbn]::text')
        loader.add_css('isbn13', 'div.infoBoxRowItem::text')
        loader.add_css('isbn13', 'script::text', re=SIMILAR_EDITIONS_ISBN_13_REGEX)

        loader.add_css('rating_histogram', 'script[type*="protovis"]::text')

        return loader.load_item()
