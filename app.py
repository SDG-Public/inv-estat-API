from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient
import csv
import os
import pandas as pd


app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   connectionString = os.environ['CUSTOMCONNSTR_storage']
   return connectionString



   
@app.route('/arnau', methods=['GET'])
def test_script():

   # Define parameters
   connectionString = os.environ['CUSTOMCONNSTR_storage']
   #connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountdessdg;AccountKey=oy2ydW+f9L+p5SLFSHXvcQsn8yzDzTbzT6YPVNItVwnznodLVYcLsR/FAkI42DSqNCoeGYfJIKXf+AStNMBovw==;EndpointSuffix=core.windows.net"

   containerName = "inversionsestat"
   inputBlobName = "Detall_SP_Admin.CSV"
   
   # DOWNLOAD
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=inputBlobName)

   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   str_datos = datos.decode('cp1252')

   llista_origen = []
   for row in iter(str_datos.splitlines()):
       llista_origen.append(row.split(';'))

   llista_final = []
   
   comunitat = "CATALUÑA"
   provincia = ""
   entidad = ""
   
   for row in llista_origen:
       if len(row) != 0 and "PROVINCIA" in row[0]:
           provincia = row[0].split(" ")[8]
       if len(row) > 2 and "ENTIDAD" in row[2]:
           aux = row[2].split(":")
           if len(aux) > 1:
               entidad = aux[1]
       if len(row) != 0 and row[0].isdigit():
           toappend = []
           toappend.extend([comunitat, provincia, entidad, row[0]])
           toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
           llista_final.append(toappend)
   
   capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI',
                'TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
   
   llista_final.insert(0, capcelera)
   
   
   anyo = llista_origen[4][0].split(' ')[4]    
    
   outputBlobName	= anyo + "_PRES_FACT_DET_SP_ADMIN.csv"
   
   # UPLOAD
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)  
   
   df = pd.DataFrame(llista_final[1:],columns = llista_final[0])
   data = df.to_csv(index=False,sep=";")

   blob.upload_blob(data,overwrite=True)
      
   return 'Blob subido'

# Con esta petición GET podemos hacer pruebas. Por ejemplo podemos crear un fichero en nuestro Blob Storage

@app.route('/test', methods=['GET'])
def query_records():
   # Define parameters
   connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountdessdg;AccountKey=oy2ydW+f9L+p5SLFSHXvcQsn8yzDzTbzT6YPVNItVwnznodLVYcLsR/FAkI42DSqNCoeGYfJIKXf+AStNMBovw==;EndpointSuffix=core.windows.net"
   containerName = "inversionsestat"
   outputBlobName	= "iris_setosa.csv"
   
   # Establish connection with the blob storage account
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
    
   data = 'esto es una prueba de blob'
    
   blob.upload_blob(data,overwrite=True)
   
   return 'Blob subido'

if __name__ == '__main__':
   app.run()

