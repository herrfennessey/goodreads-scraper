import json
import os
import subprocess
import uuid
from datetime import datetime
from typing import List, Optional

from flask import Flask, jsonify
from flask_pydantic import validate
from google.cloud import bigquery
from pydantic import BaseModel, parse_obj_as

from dao.big_query_dao import BigQueryDao
from models.user_review_bigquery_dto import UserReviewBigQueryDto
from models.user_scrape_request import UserScrapeRequest

app = Flask(__name__)
bq = BigQueryDao()


@app.route('/scrape-users', methods=['POST'])
@validate()
def scrape_user_profiles(body: UserScrapeRequest):
    comma_delimited_profiles = ",".join(body.profiles)
    app.logger.info(f"Processing user batch: {body.profiles}")

    # Be super careful here. I am using GCP Cloud Run to limit concurrency to 1 per node. If you don't have an external
    # mechanism, you will potentially start a ton of scrapers which will compete for resources and also not respect your
    # desired requests per second. If you wanted to purely solve this in python, ou could use semaphores or uwsgi
    # to limit concurrency
    output_file = f"{uuid.uuid4()}.json"
    response = dict()
    try:
        subprocess.check_output(
            ['scrapy', 'crawl', "user_reviews", "-o", output_file, "-a", f"profiles={comma_delimited_profiles}"])
        with open(output_file) as results:
            result_json_list = json.load(results)
            if body.debug:
                response["debug"] = result_json_list
            else:
                user_review_list = parse_obj_as(List[UserReviewBigQueryDto], result_json_list)
                bq.write(user_review_list)
    except Exception as e:
        app.logger.info(f"Encountered exception: {e}")
    finally:
        os.remove(output_file)
        return jsonify(response)


@app.route('/', methods=['GET'])
def hello_world():
    return jsonify({"hello": "world"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=9000, processes=1)
