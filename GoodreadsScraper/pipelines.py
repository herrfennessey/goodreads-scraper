# -*- coding: utf-8 -*-
import json
import logging
from typing import List

from google.cloud import tasks_v2
from scrapy import signals, Field
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exporters import JsonLinesItemExporter

from GoodreadsScraper.items import UserProfileItem

logger = logging.getLogger(__name__)


class JsonLineItemSegregator(object):
    @classmethod
    def from_crawler(cls, crawler):
        output_file_suffix = crawler.settings.get("OUTPUT_FILE_SUFFIX", default="")
        return cls(crawler, output_file_suffix)

    def __init__(self, crawler, output_file_suffix):
        self.types = {"book", "author", "userprofile", "userreview"}
        self.output_file_suffix = output_file_suffix
        self.files = set()
        self.exporters = None
        crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        self.files = {name: open(name + "_" + self.output_file_suffix + '.jl', 'a+b') for name in self.types}
        self.exporters = {name: JsonLinesItemExporter(self.files[name]) for name in self.types}

        for e in self.exporters.values():
            e.start_exporting()

    def spider_closed(self, spider):
        for e in self.exporters.values():
            e.finish_exporting()

        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        item_type = type(item).__name__.replace("Item", "").lower()
        if item_type in self.types:
            self.exporters[item_type].export_item(item)
        return item


class GcpTaskQueuePipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def __init__(self, crawler):
        self.pipeline_name = "profile-scrape-queue"
        self.http_target = "https://user-review-scraper-pool-tmzffqb3oq-ue.a.run.app/scrape-users"
        self.client = None
        self.parent = None
        self.item_list: List[UserProfileItem] = []
        self.number_of_profiles_per_crawl = 5
        crawler.signals.connect(self.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        self.client = tasks_v2.CloudTasksClient()
        self.parent = self.client.queue_path("book-suggestion-please", "us-east1", self.pipeline_name)

    def spider_closed(self, spider):
        self.send_task()

    def process_item(self, item, spider):
        self.item_list.append(item)
        if len(self.item_list) >= self.number_of_profiles_per_crawl:
            self.send_task()

    def send_task(self):
        task = {
            "http_request": {  # Specify the type of request.
                "http_method": tasks_v2.HttpMethod.POST,
                "url": self.http_target,  # The full url path that the task will be sent to.
            }
        }
        if len(self.item_list) > 0:
            user_profile_list = [item['profile_url'] for item in self.item_list]
            # Convert list to JSON query
            payload = json.dumps({"profiles": user_profile_list})
            # specify http content-type to application/json
            task["http_request"]["headers"] = {"Content-type": "application/json"}

            # The API expects a payload of type bytes.
            converted_payload = payload.encode()

            # Add the payload to the request.
            task["http_request"]["body"] = converted_payload

            response = self.client.create_task(request={"parent": self.parent, "task": task})
            logger.info("Created task {}".format(response.name))
            self.item_list = []
