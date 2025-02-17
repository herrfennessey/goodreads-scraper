"""Spider to extract information from a /book/show type page on Goodreads"""
import json
import re
from urllib.parse import urlsplit

import scrapy
from scrapy import Request

from ..items import LegacyBookItem, BookLoader, BookItem

TYPENAME = "__typename"

SIMILAR_EDITIONS_ISBN_REGEX = re.compile(r"editionInfo\\'>\\nisbn:\s(\d+)\\n<\\")
SIMILAR_EDITIONS_ISBN_13_REGEX = re.compile(r"editionInfo\\'>\\nisbn13:\s(\d+)\\n<\\")
SIMILAR_EDITIONS_ASIN_REGEX = re.compile(r"editionInfo\\'>\\nasin:\s([A-Za-z0-9]+)\\n<\\")


class BookSpider(scrapy.Spider):
    """Extract information from a /book/show type page on Goodreads"""
    name = "book"

    def __init__(self, books="/book/show/12232938-the-lovely-bones"):
        super().__init__()
        self.start_urls = books.split(",")

    def start_requests(self):
        for url in self.start_urls:
            converted_url = self._format_book_url(url)
            yield Request(converted_url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        if response.selector.attrib.get('class', "").startswith("desktop withSiteHeaderTopFullImage"):
            self.logger.info("Legacy response")
            return self.parse_legacy_book(response)
        else:
            self.logger.info("New Book Response")
            return self.parse_book(response)

    def parse_book(self, response):
        loader = BookLoader(BookItem(), response=response)

        text_body = response.xpath('//*[@id="__NEXT_DATA__"]/text()').get()
        parsed_json_body = json.loads(text_body)
        book_info = parsed_json_body['props']['pageProps']['apolloState']

        contributor = self._take_largest_element(book_info, "Contributor")
        series = self._take_first_element(book_info, "Series")
        work = self._take_largest_element(book_info, "Work")
        book = self._take_largest_element(book_info, "Book")

        url = urlsplit(response.request.url).path

        if not book:
            self.logger.warning("Unable to load body, reloading page!")
            converted_url = self._format_book_url(url)
            return Request(converted_url, callback=self.parse, dont_filter=True)

        loader.add_value('url', url)
        loader.add_value('title', book.get("title"))
        loader.add_value('author', contributor.get("name"))
        loader.add_value('author_url', contributor.get("webUrl"))
        loader.add_value('num_ratings', work.get("stats").get("ratingsCount"))
        loader.add_value('num_reviews', work.get("stats").get("textReviewsCount"))
        loader.add_value('avg_rating', work.get("stats").get("averageRating"))
        loader.add_value('num_pages', book.get("details").get("numPages"))
        loader.add_value('language', book.get("details").get("language").get("name"))
        loader.add_value('publish_date', book.get("details").get("publicationTime"))
        loader.add_value('isbn', book.get("details").get("isbn"))
        loader.add_value('isbn13', book.get("details").get("isbn13"))
        loader.add_value('asin', book.get("details").get("asin"))
        loader.add_value('series', series.get("title") if series else "")
        loader.add_value('genres', self._parse_genres(book.get("bookGenres")))
        loader.add_value('rating_histogram', work.get("stats").get("ratingsCountDist"))

        return loader.load_item()

    def parse_legacy_book(self, response):
        loader = BookLoader(LegacyBookItem(), response=response)

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

    def _take_largest_element(self, input_dict, element_type):
        largest = None
        for block in input_dict.values():
            if block.get(TYPENAME, "") == element_type:
                if largest is None:
                    largest = block
                else:
                    key_count = self._count_keys_recursive(block)
                    largest_count = self._count_keys_recursive(largest)
                    if key_count > largest_count:
                        largest = block
            else:
                continue
        return largest

    def _parse_genres(self, genre_input_list):
        parsed_genres = []
        for genre in genre_input_list:
            if genre.get(TYPENAME) == "BookGenre":
                genre_dict = genre.get("genre")
                if genre_dict.get(TYPENAME) == "Genre":
                    parsed_genres.append(genre_dict.get("name"))
        return parsed_genres

    def _take_first_element(self, input_dict, element_type):
        for block in input_dict.values():
            if block.get(TYPENAME, "") == element_type:
                return block

    def _count_keys_recursive(self, input_dict, counter=0):
        for each_key in input_dict:
            if isinstance(input_dict[each_key], dict):
                # Recursive call
                counter = self._count_keys_recursive(input_dict[each_key], counter + 1)
            else:
                counter += 1
        return counter

    @staticmethod
    def _format_book_url(user_id_and_name):
        return f"https://www.goodreads.com{user_id_and_name}"
