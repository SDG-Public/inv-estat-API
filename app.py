from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
import json
from azure.storage.blob import BlobClient
import csv
import os
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext


app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"



# Descargamos los ficheros del SharePoint
@app.route('/download')
def download_files():
   

   # Parametros de conexión a SharePoint
   baseurl = 'https://gencat.sharepoint.com'
   basesite = '/sites/Provespython' 
   siteurl = baseurl + basesite
   relative_url = f'Shared Documents/'

   username = os.environ['CUSTOMCONNSTR_username'] #config.username
   pwd = os.environ['CUSTOMCONNSTR_password'] #config.password
   
   ctx_auth = AuthenticationContext(siteurl) # should also be the siteurl
   ctx_auth.acquire_token_for_user(username, pwd)
   ctx = ClientContext(siteurl, ctx_auth) # make sure you auth to the siteurl.
   
   
   
   # Parametros de conexion Blob Storage
   connectionString = os.environ['CUSTOMCONNSTR_storage']
   containerName = "inversionsestat"
   
   with open(os.path.join(app.root_path, 'lista_ficheros.lst'),'r') as f:
      lista_ficheros = f.read().splitlines()
    
   for fichero in lista_ficheros: 

      # Bajada y subida de fichero
      relative_file_path = f'/sites/Provespython/Shared Documents/'
      down_file_path = relative_file_path + fichero
      
      # Creamos una conexión con un nuevo nombre de destino
      blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName,
                                            blob_name=fichero)
      
      response = File.open_binary(ctx, down_file_path)
      blob.upload_blob(response.content, overwrite=True)
                                         
   
   return "Archivos descargados correctamente"










# Aquí determinamos el metodo GET de la URL /SP_Admin
@app.route('/SP_Admin', methods=['GET'])
def SP_Admin_script():

   # Definimos los parametros de nuestro Blob Storage
   connectionString = os.environ['CUSTOMCONNSTR_storage']
   containerName = "inversionsestat"
   inputBlobName = "Detall_SP_Admin.CSV"
   
   # Nos conectamos al Blob Storage
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=inputBlobName)
    
   # Descargamos el fichero y guardamos su valor en una variable
   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   # Decodificamos los datos ANSI (cp1252)
   str_datos = datos.decode('cp1252')
    
   # A partir de los datos generamos una lista
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
   
   # Creamos una conexión con un nuevo nombre de destino
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)  
   
   # Cargamos los datos a un dataframe
   df = pd.DataFrame(llista_final[1:],columns = llista_final[0])
   data = df.to_csv(index=False,sep=";")
   # Los subimos a Blob Storage
   blob.upload_blob(data,overwrite=True)
      
   return 'Blob subido'

# Con esta petición GET podemos hacer pruebas. Por ejemplo podemos crear un fichero en nuestro Blob Storage

@app.route('/test', methods=['GET'])
def test_script():
   # Define parameters
   connectionString = os.environ['CUSTOMCONNSTR_storage']
   containerName = "inversionsestat"
   outputBlobName	= "test.csv"
   
   # Establish connection with the blob storage account
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)
    
   data = 'Esto es una prueba de blob'
    
   blob.upload_blob(data,overwrite=True)
   
   return 'Blob subido'

# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

