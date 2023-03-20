from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient
import csv
import os
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from office365.runtime.auth.authentication_context import AuthenticationContext

app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"

# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

