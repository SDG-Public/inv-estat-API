#!/usr/bin/env python
# encoding: utf-8
import json
from flask import Flask
app = Flask(__name__)

@app.route('/')


def index():
    return json.dumps({'name': 'alice',
                       'email': 'alice@outlook.com'})
                       
f = open("demofile2.txt", "a")
f.write("Now the file has more content!")
f.close()


app.run()