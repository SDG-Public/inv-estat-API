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


# 1.1
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\1. Estado, Organismos\Agrupación
# Aquí determinamos el metodo GET de la URL /CCAA_Ministeris
@app.route('/CCAA_Ministeris', methods=['GET'])
def CCAA_Ministeris_script():

   llista_origen = []
   llista_any = []
   llista_seccions = []
   
   csv_origen = descarga_blob('Resum_Ministeris.CSV')
   
   # Obrim el fitxer i introduïm registres

   for x,row in enumerate(csv_origen):
       if 6 < x < 83:
           llista_origen.append(row)

   for row in csv_origen:
       llista_any.append(row)
   
   
   anyo = llista_any[3][1].split(' ')[2]
   
   # Creem una llista amb els ministeris - seccions
   llista_seccions = llista_origen[0]
   llista_seccions.pop(0)
   llista_seccions.remove('')
   llista_seccions.remove('Total')
   llista_seccions.pop(31)
   llista_seccions.pop(30)
   llista_seccions.pop(29)
   
   # Creem una llista amb les denominacions i els diners
   
   llista_origen.pop(0)
   
   # Traiem els identificadors
   for x in range(75):
       llista_origen[x].pop(0)
   
   # Llista CCAA per treure-les
   llista_CCAA = ['PAIS VASCO', 'CATALUÑA', 'GALICIA', 'ANDALUCIA', 'ASTURIAS', 'CANTABRIA', 'LA RIOJA',
                  'REGION DE MURCIA', 'COMUNIDAD VALENCIANA', 'ARAGON', 'CASTILLA-LA MANCHA', 'CANARIAS', 'NAVARRA',
                  'EXTREMADURA', 'BALEARS', 'MADRID', 'CASTILLA Y LEON', 'NO REGIONALIZABLE',
                  'EXTRANJERO']
   
   llista_sense_CCAA = []
   

   for x,row in enumerate(llista_origen):
       if llista_origen[x][0] not in llista_CCAA:
           llista_sense_CCAA.append(row)
   
   # Ajuntem les llistes per tenir tota la info
   
   llista_sense_CCAA.insert(0, llista_seccions)
   
   llista = llista_sense_CCAA
   
   # Creem un procés que iteri cada quadrat, n'extregui la info de la seva respectiva primera fila i columna i les posi en una llista de 3.
   
   llista_final = []
   x = 0
   y = 0
   
   for x in range(1,23):
       for y in range(1, 57):
           llista_tupla = [llista[0][x], llista[y][0], llista[y][x]]
           llista_final.append(llista_tupla)
   
   capcelera = ['MINISTERI', 'COMUNIDAD AUTONOMA', 'COST TOTAL']
   llista_final.insert(0, capcelera)
   
   upload_file = anyo + '_PRES_FACT_AGR_MIN_EST_OOAA_RE.csv'
   subida_blob(upload_file,llista_final)
   
   return 'Blob CCAA_Ministeris subido'

  


# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

