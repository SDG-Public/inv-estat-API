from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
app = Flask(__name__)


@app.route('/')
def index():
   print('Request for index page received')
   return json.dumps({'name': 'alice',
                       'email': 'alice@outlook.com'})


if __name__ == '__main__':
   app.run()