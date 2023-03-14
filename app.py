from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient


app = Flask(__name__)


@app.route('/')
def index():
   # Define parameters
   connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountdessdg;AccountKey=oy2ydW+f9L+p5SLFSHXvcQsn8yzDzTbzT6YPVNItVwnznodLVYcLsR/FAkI42DSqNCoeGYfJIKXf+AStNMBovw==;EndpointSuffix=core.windows.net"
   containerName = "inversionsestat"
   outputBlobName	= "iris_setosa.csv"
   
   # Establish connection with the blob storage account
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
    
   data = 'esto es una prueba de blob'
    
   blob.upload_blob(data)
   
   return json.dumps({'name': 'alice',
                       'email': 'alice@outlook.com'})


if __name__ == '__main__':
   app.run()

