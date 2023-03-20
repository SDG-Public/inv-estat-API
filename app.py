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


# 1.1
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\1. Estado, Organismos\Detalle
# Aquí determinamos el metodo GET de la URL /Estado_org
@app.route('/Estado_org', methods=['GET'])
def Estado_org_script():

    llista_108 = descarga_blob('N_22_E_V_2_R_1_202_1_108_1.CSV')
    llista_114 = descarga_blob('N_22_E_V_2_R_1_202_1_114_1.CSV')
    llista_115 = descarga_blob('N_22_E_V_2_R_1_202_1_115_1.CSV')
    llista_116 = descarga_blob('N_22_E_V_2_R_1_202_1_116_1.CSV')
    llista_117 = descarga_blob('N_22_E_V_2_R_1_202_1_117_1.CSV')
    #llista_118 = descarga_blob('N_22_E_V_2_R_1_202_1_118_1.CSV')
    llista_119 = descarga_blob('N_22_E_V_2_R_1_202_1_119_1.CSV')
    llista_120 = descarga_blob('N_22_E_V_2_R_1_202_1_120_1.CSV')
    llista_123 = descarga_blob('N_22_E_V_2_R_1_202_1_123_1.CSV')
    llista_124 = descarga_blob('N_22_E_V_2_R_1_202_1_124_1.CSV')
    llista_128 = descarga_blob('N_22_E_V_2_R_1_202_1_128_1.CSV')
    llista_final = []

    def individual(llista):
        tot_ministeri = llista[3][1].split(':')[1]
        ministeri = tot_ministeri[4:]
        id_ministeri = tot_ministeri.split(' ')[1]
        Id_CCAA = llista[4][1].split(':')[1]
        CCAA = Id_CCAA.split(' ')[2]
        x = 0
        id_org = ''
        desc_org = ''
        idprograma = ''
        article = ''
        for x,row in enumerate(llista):
            if row != [] and (len(llista)-7) > x > 10 and row[4] != 'TOTAL' and (row[11] != '' or row[12] != '' or
            row[13] != '' or row[14] != '' or row[15] != '' or row[10] != ''):
                if row[0] != '':
                id_org = row[0]
                desc_org = row[4]
                if row[1] != '':
                    idprograma = row[1]
                if row[2] != '':
                    article = row[2]
                if row[3] != '':
                    toappend = []
                    toappend.extend([id_ministeri, ministeri, CCAA, id_org, idprograma, article, desc_org])
                    toappend.extend(list(row[i] for i in [3, 4, 5, 6, 8, 9, 10, 11, 12, 13, 14, 15]))
                    llista_final.append(toappend)
    
    
    individual(llista_108)
    individual(llista_114)
    individual(llista_115)
    individual(llista_116)
    individual(llista_117)
    individual(llista_119)
    individual(llista_120)
    individual(llista_123)
    individual(llista_124)
    individual(llista_128)
    
    
    capcelera = ['ID_MINISTERI','DESC_MINISTERI' ,'COMUNITAT_AUTONOMA', 'CODI_CENTRE','ID_PROGRAMA', 'ID_ARTICLE',
                'DESC_CENTRE','ID_PROJECTE', 'NOM_PROJECTE',
                'ANY_INICI', 'ANY_FI', 'PROVINCIA', 'TIPUS', 'COST_TOTAL', 'ANY_ANTERIOR', 'ANY_ACTUAL',
                'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    
    llista_final.insert(0, capcelera)
    
    anyo = llista_108[5][1].split(' ')[2]

    upload_file = anyo + '_PRES_FACT_AGR_MIN_EST_OOAA_RE.csv'
    subida_blob(upload_file,llista_final)
    
    return 'Blob Estado Org subido'
  



# 1.2
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\2. Resumenes de inversiones\Agrupación
# Aquí determinamos el metodo GET de la URL /Resum_inv
@app.route('/Resum_inv', methods=['GET'])
def Resum_inv_script():

    
    # Creem les llistes buides
    estado=[]
    ooaa=[]
    restoent=[]
    ss_ss=[]
    
    # Obrim els 4 arxius
    # Només agafarem la part de la informació interessant, juntament amb l'any i el tipus d'inversió, els quals trobem a les fileres 2 i 3 del csv.
    
    # Primer fitxer
    csv_reader_estado = descarga_blob('ESTADO.CSV')

    any = ''
    tipus_inversio = ''

    for x,row in enumerate(csv_reader_estado):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                any = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                estado.append([row[0], row[1], any, tipus_inversio])
    
    # Segon fitxer
    csv_reader_ooaa = descarga_blob('OOAA.CSV')
    any = ''
    tipus_inversio = ''
    for x,row in enumerate(csv_reader_ooaa):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                any = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                ooaa.append([row[0], row[1], any, tipus_inversio])
    
    # Tercer fitxer
    csv_reader_restoent = descarga_blob('RESTOENT.CSV')
    any = ''
    tipus_inversio = ''
    
    for x,row in enumerate(csv_reader_restoent):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                any = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                restoent.append([row[0], row[1], any, tipus_inversio])
                
    # Quart fitxer             
    csv_reader_ss_ss = descarga_blob('SS_SS.CSV')
    any = ''
    tipus_inversio = ''
    for x,row in enumerate(csv_reader_ss_ss):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                any = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                ss_ss.append([row[0], row[1], any, tipus_inversio])
            
    
    # Seguidament, seleccionarem només les comunitats autònomes ignorant les províncies.
    
    llista_CCAA = ['PAIS VASCO', 'CATALUÑA', 'GALICIA', 'ANDALUCIA', 'ASTURIAS', 'CANTABRIA', 'LA RIOJA',
                'REGION DE MURCIA', 'COMUNIDAD VALENCIANA', 'ARAGON', 'CASTILLA-LA MANCHA', 'CANARIAS', 'NAVARRA',
                'EXTREMADURA', 'BALEARS', 'MADRID', 'CASTILLA Y LEON', '  CEUTA', 'MELILLA', 'NO REGIONALIZABLE',
                'EXTRANJERO']
    
    # Unifiquem les 4 llistes i creem un bucle per ignorar les províncies.
    llista_union = estado + ooaa + restoent + ss_ss
    
    llista_final = []
    
    for x,row in enumerate(llista_union):
        if llista_union[x][0] in llista_CCAA:
            llista_final.append(row)

    
    capcelera = ['COMUNITAT_AUTONOMA', 'COST_TOTAL', 'ANY_EXC_PRESUPOSTARI', 'INVERSIO']
    llista_final.insert(0, capcelera)
    
    
    upload_file = str(any)+'_PRES_FACT_AGR_RESUMEN_INVERSIONS.csv'
    subida_blob(upload_file,llista_final)
    
    return 'Blob Resumen Inv subido'




# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

