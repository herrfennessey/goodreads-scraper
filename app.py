# server.py
import subprocess

from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    """
    Run spider in another process and store items in file. Simply issue command:

    wait for  this command to finish, and read output.json to client.
    """
    spider_name = "user_reviews"
    subprocess.check_output(['scrapy', 'crawl', spider_name, "-o", "output.json"])
    with open("output.json") as items_file:
        return items_file.read()

if __name__ == '__main__':
    app.run(debug=True)