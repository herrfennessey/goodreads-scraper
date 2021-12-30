# server.py
import os
import subprocess
import uuid
from typing import List

from flask import Flask, request, jsonify
from flask_pydantic import validate
from pydantic import BaseModel

app = Flask(__name__)


class UserScrapeRequest(BaseModel):
    profiles: List[str]


@app.route('/scrape-users', methods=['POST'])
@validate()
def scrape_user_profiles(body: UserScrapeRequest):
    request_data = request.get_json()
    comma_delimited_profiles = ",".join(body.profiles)
    app.logger.info(f"Processing user batch: {body.profiles}")

    # Be super careful here. I am using GCP Cloud Run to limit concurrency to 1 per node. If you don't have an external
    # mechanism, you will potentially start a ton of scrapers which will compete for resources and also not respect your
    # desired requests per second. If you wanted to purely solve this in python, ou could use semaphores or uwsgi
    # to limit concurrency
    output_file = f"{uuid.uuid4()}.json"
    response = "{}"
    try:
        subprocess.check_output(
            ['scrapy', 'crawl', "user_reviews", "-o", output_file, "-a", f"profiles={comma_delimited_profiles}"])
        with open(output_file) as items_file:
            response = items_file.read()
    except Exception as e:
        app.logger.info(f"Encountered exception: {e}, skipping")
    finally:
        os.remove(output_file)
        return jsonify(response)


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=9000, processes=1)
