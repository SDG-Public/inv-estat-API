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


# Parametros de conexion Blob Storage
connectionString = os.environ['CUSTOMCONNSTR_storage']
containerName = "inversionsestat"


def descarga_blob(download_file):
   # Nos conectamos al Blob Storage
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=download_file)
    
   # Descargamos el fichero y guardamos su valor en una variable
   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   # Decodificamos los datos ANSI (cp1252)
   str_datos = datos.decode('cp1252')
    
   # A partir de los datos generamos una lista
   llista_descarregada = []
   for row in iter(str_datos.splitlines()):
       llista_descarregada.append(row.split(';'))
       
   return llista_descarregada


def descarga_excel(download_file,posicions_excel):
   # Nos conectamos al Blob Storage
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=download_file)
    
   # Descargamos el fichero y guardamos su valor en una variable
   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   # read the excel file
   df = pd.read_excel(datos, posicions_excel)
       
   return df

def subida_blob(upload_file,llista_final):
   # Creamos una conexión con un nuevo nombre de destino
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=upload_file)  
   
   # Cargamos los datos a un dataframe
   df = pd.DataFrame(llista_final[1:],columns = llista_final[0])
   data = df.to_csv(index=False,sep=";")
   # Los subimos a Blob Storage
   blob.upload_blob(data,overwrite=True)

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

  


# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

