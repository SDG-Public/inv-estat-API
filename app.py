from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient
import csv

app = Flask(__name__)


@app.route('/')
def index():
   
   return json.dumps({'name': 'alice',
                       'email': 'alice@outlook.com'})


@app.route('/run', methods=['GET'])
def query_records():
   # Define parameters
   connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountdessdg;AccountKey=oy2ydW+f9L+p5SLFSHXvcQsn8yzDzTbzT6YPVNItVwnznodLVYcLsR/FAkI42DSqNCoeGYfJIKXf+AStNMBovw==;EndpointSuffix=core.windows.net"
   containerName = "inversionsestat"
   outputBlobName	= "iris_setosa.csv"
   
   # Establish connection with the blob storage account
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
    
   data = 'esto es una prueba de blob'
    
   blob.upload_blob(data)
   
   return 'Blob subido'
   
@app.route('/arnau', methods=['GET'])
def test_script():

   # Define parameters
   connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountdessdg;AccountKey=oy2ydW+f9L+p5SLFSHXvcQsn8yzDzTbzT6YPVNItVwnznodLVYcLsR/FAkI42DSqNCoeGYfJIKXf+AStNMBovw==;EndpointSuffix=core.windows.net"
   containerName = "inversionsestat"
   inputBlobName = "Detall_SP_Admin.CSV"
   
   # DOWNLOAD
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=inputBlobName)
   blob.download_blob()
   
   with open(file=inputBlobName, mode="wb") as sample_blob:
      download_stream = blob.download_blob()
      sample_blob.write(download_stream.readall())
   
   llista_origen = []
   
   #
   #
   # HASTA AQUI FUNIONA OK!
   #
   #
     
   
   #with open(file=inputBlobName, 'r') as csv_origen:
   #    csv_reader = csv.reader(csv_origen, delimiter=';')
   #    for row in csv_reader:
   #        llista_origen.append(row)
   #
   #
   
   
   
   
   #llista_final = []
   #
   #comunitat = "CATALUÑA"
   #provincia = ""
   #entidad = ""

   #for row in llista_origen:
   #    if len(row) != 0 and "PROVINCIA" in row[0]:
   #        provincia = row[0].split(" ")[8]
   #    if len(row) > 2 and "ENTIDAD" in row[2]:
   #        aux = row[2].split(":")
   #        if len(aux) > 1:
   #            entidad = aux[1]
   #    if len(row) != 0 and row[0].isdigit():
   #        toappend = []
   #        toappend.extend([comunitat, provincia, entidad, row[0]])
   #        toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
   #        llista_final.append(toappend)
   #
   #capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI',
   #             'TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
   #
   #llista_final.insert(0, capcelera)
   #
   #
   #anyo = llista_origen[4][0].split(' ')[4]    
    
   outputBlobName	= "test_cancer_PRES_FACT_DET_SP_ADMIN.csv"
      
   ## UPLOAD
   #blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)   
   #
   #with open(outputBlobName,"w") as csv_file:
   #    writer = csv.writer(csv_file, delimiter=';')
   #    for line in llista_final:
   #        writer.writerow(line)

   #with open(outputBlobName, "r") as data:
   #   blob.upload_blob(data)
      
   return 'Blob subido'

if __name__ == '__main__':
   app.run()

