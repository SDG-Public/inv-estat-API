#!/usr/bin/env python
# encoding: utf-8
import json
from flask import Flask
app = Flask(__name__)

@app.route('/')


def index():
    return '<p> te devuelvo un texto </p>'
                       
#print('asa')


app.run()
