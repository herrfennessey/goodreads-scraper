# -*- coding: utf-8 -*-

import datetime
# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import json
import re

import scrapy
from dateutil.parser import parse as dateutil_parse
from scrapy import Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Compose, MapCompose, TakeFirst, Join
from w3lib.html import remove_tags

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def num_page_extractor(num_pages):
    if num_pages:
        return num_pages.split()[0]
    return None


def safe_parse_date(date):
    try:
        date = dateutil_parse(date, fuzzy=True, default=datetime.datetime.min)
        date = date.strftime(TIME_FORMAT)
    except ValueError:
        date = None

    return date


def extract_legacy_publish_date(maybe_dates):
    maybe_dates = [s for s in maybe_dates if "published" in s.lower()]
    return [safe_parse_date(date) for date in maybe_dates]


def extract_legacy_publish_year(s):
    s = s.lower().strip()
    match = re.match(".*first published.*(\d{4})", s)
    if match:
        return match.group(1)


def extract_legacy_ratings(txt):
    """Extract the rating histogram from embedded Javascript code

        The embedded code looks like this:

        |----------------------------------------------------------|
        | renderRatingGraph([6, 3, 2, 2, 1]);                      |
        | if ($('rating_details')) {                               |
        |   $('rating_details').insert({top: $('rating_graph')})   |
        |  }                                                       |
        |----------------------------------------------------------|
    """
    codelines = "".join(txt).split(";")
    rating_code = [line.strip() for line in codelines if "renderRatingGraph" in line]
    if not rating_code:
        return None
    rating_code = rating_code[0]
    rating_array = rating_code[rating_code.index("[") + 1: rating_code.index("]")]
    ratings = {5 - i: int(x) for i, x in enumerate(rating_array.split(","))}
    return ratings


def extract_legacy_ratings_as_json(txt):
    return json.dumps(extract_legacy_ratings(txt))


def extract_ratings_as_json(rating_list):
    rating_dict = {idx + 1: rating for idx, rating in enumerate(rating_list)}
    return json.dumps(extract_legacy_ratings(rating_dict))


def filter_asin(asin):
    if asin and len(str(asin)) == 10:
        return asin
    return None


def isbn_filter(isbn):
    if isbn and len(str(isbn)) == 10 and isbn.isdigit():
        return isbn


def isbn13_filter(isbn):
    if isbn and len(str(isbn)) == 13 and isbn.isdigit():
        return isbn


def filter_empty(vals):
    return [v.strip() for v in vals if v.strip()]


def split_by_newline(txt):
    return txt.split("\n")


def extract_language(txt):
    return txt.split(";")[0]


def convert_epoch_to_timestamp(epoch):
    time_object = datetime.datetime.fromtimestamp(epoch)
    return time_object.strftime(TIME_FORMAT)


def extract_year_from_timestamp(epoch):
    time_object = datetime.datetime.fromtimestamp(epoch)
    return time_object.getYear()


class LegacyBookItem(scrapy.Item):
    # Scalars
    url = Field()

    title = Field(input_processor=MapCompose(str.strip))
    author = Field(input_processor=MapCompose(str.strip))
    author_url = Field(input_processor=MapCompose(str.strip))

    num_ratings = Field(input_processor=MapCompose(str.strip, int))
    num_reviews = Field(input_processor=MapCompose(str.strip, int))
    avg_rating = Field(input_processor=MapCompose(str.strip, float))
    num_pages = Field(input_processor=MapCompose(str.strip, num_page_extractor, int))

    language = Field(input_processor=MapCompose(str.strip))
    publish_date = Field(input_processor=extract_legacy_publish_date)

    original_publish_year = Field(input_processor=MapCompose(extract_legacy_publish_year, int))

    isbn = Field(input_processor=MapCompose(str.strip, isbn_filter))
    isbn13 = Field(input_processor=MapCompose(str.strip, isbn13_filter))
    asin = Field(input_processor=MapCompose(filter_asin))

    series = Field()

    # Lists
    genres = Field(output_processor=Compose(set, list))

    # Dicts
    rating_histogram = Field(input_processor=MapCompose(extract_legacy_ratings_as_json))


class LegacyBookLoader(ItemLoader):
    default_output_processor = TakeFirst()


class BookItem(scrapy.Item):
    # Scalars
    url = Field()

    title = Field(input_processor=MapCompose(str.strip))
    author = Field(input_processor=MapCompose(str.strip))
    author_url = Field(input_processor=MapCompose(str.strip))

    num_ratings = Field()
    num_reviews = Field()
    avg_rating = Field()
    num_pages = Field()

    language = Field(input_processor=MapCompose(extract_language))
    publish_date = Field(input_processor=convert_epoch_to_timestamp)

    original_publish_year = Field(input_processor=MapCompose(extract_year_from_timestamp, int))

    isbn = Field(input_processor=MapCompose(str.strip, isbn_filter))
    isbn13 = Field(input_processor=MapCompose(str.strip, isbn13_filter))
    asin = Field(input_processor=MapCompose(filter_asin))

    series = Field(input_processor=MapCompose(str.strip))

    # Lists
    genres = Field(output_processor=Compose(set, list))

    # Dicts
    rating_histogram = Field(input_processor=MapCompose(extract_ratings_as_json))


class UserProfileItem(scrapy.Item):
    profile_url = Field()


class AuthorItem(scrapy.Item):
    # Scalars
    url = Field()

    name = Field()
    birth_date = Field(input_processor=MapCompose(safe_parse_date))
    death_date = Field(input_processor=MapCompose(safe_parse_date))

    avg_rating = Field(serializer=float)
    num_ratings = Field(serializer=int)
    num_reviews = Field(serializer=int)

    # Lists
    genres = Field(output_processor=Compose(set, list))
    influences = Field(output_processor=Compose(set, list))

    # Blobs
    about = Field(
        # Take the first match, remove HTML tags, convert to list of lines, remove empty lines, remove the "edit data" prefix
        input_processor=Compose(TakeFirst(), remove_tags, split_by_newline, filter_empty, lambda s: s[1:]),
        output_processor=Join()
    )


class AuthorLoader(ItemLoader):
    default_output_processor = TakeFirst()


class UserReviewItem(scrapy.Item):
    user_id = Field()
    user_id_slug = Field()

    book_link = Field()
    book_name = Field()

    author_link = Field()
    author_name = Field()

    date_read = Field(input_processor=MapCompose(safe_parse_date))
    date_added = Field(input_processor=MapCompose(safe_parse_date))

    user_rating = Field(serializer=int)


class UserReviewLoader(ItemLoader):
    default_output_processor = TakeFirst()
