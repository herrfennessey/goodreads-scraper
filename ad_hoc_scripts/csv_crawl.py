import argparse
import csv
import json
import logging

from google.cloud import tasks_v2

logger = logging.getLogger(__name__)

NUMBER_OF_BOOKS_PER_CRAWL = 60


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class BookScrapeEnqueuer(object):

    def __init__(self, input_file):
        self.pipeline_name = "profile-scrape-queue"
        self.http_target = "https://user-review-scraper-pool-tmzffqb3oq-ue.a.run.app/scrape-books"
        self.client = tasks_v2.CloudTasksClient()
        self.parent = self.client.queue_path("book-suggestion-please", "us-east1", self.pipeline_name)
        self.input_file = input_file

    def enqueue_books(self):
        items = []

        with open(self.input_file) as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=',')
            for row in csv_reader:
                items.append(row['book_link'])

        print(len(items))

        for chunk in chunks(items, NUMBER_OF_BOOKS_PER_CRAWL):
            self.send_task(chunk)

        print("Done!")

    def send_task(self, book_chunk):
        task = {
            "http_request": {  # Specify the type of request.
                "http_method": tasks_v2.HttpMethod.POST,
                "url": self.http_target,  # The full url path that the task will be sent to.
            }
        }
        if len(book_chunk) > 0:
            payload = json.dumps({"book_urls": book_chunk})
            # specify http content-type to application/json
            task["http_request"]["headers"] = {"Content-type": "application/json"}

            # The API expects a payload of type bytes.
            converted_payload = payload.encode()

            # Add the payload to the request.
            task["http_request"]["body"] = converted_payload

            response = self.client.create_task(request={"parent": self.parent, "task": task})
            print("Created task {}".format(response.name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Book loader')
    parser.add_argument('--book-file', help='please give the csv file to load to be scraped')
    args = parser.parse_args()

    enqueuer = BookScrapeEnqueuer(args.book_file)
    enqueuer.enqueue_books()
