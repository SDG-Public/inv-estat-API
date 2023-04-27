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
import re

# Parametros de conexion Blob Storage
connectionString = os.environ['CUSTOMCONNSTR_storage']
containerName = "inversionsestat"


# Parametros de conexión a SharePoint
baseurl = 'https://gencat.sharepoint.com'
basesite =  os.environ['CUSTOMCONNSTR_basesite'] #'/sites/ProvespythonInvEstat' 
siteurl = baseurl + basesite
relative_file_path = basesite + '/Documents compartits/'
relative_file_path_no_slash =  relative_file_path[:-2]
username = os.environ['CUSTOMCONNSTR_username'] #config.username
pwd = os.environ['CUSTOMCONNSTR_password'] #config.password


def descarga_blob(download_file):
   # Nos conectamos al Blob Storage
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=download_file)
    
   # Descargamos el fichero y guardamos su valor en una variable
   download_stream = blob.download_blob()
   datos = download_stream.readall()
   
   # Decodificamos los datos ANSI (cp1252)
   try:
      str_datos = datos.decode('cp1252')
    #Decodificamos los datos UTF8 si el otro encoding falla
   except:
      str_datos = datos.decode('utf8')
   
   # Eliminamos salos de linea que pueda haber entre comillas
   pattern = re.compile(r'".*?"', re.DOTALL)
   str_datos = pattern.sub(lambda x: x.group().replace('\n', ' '), str_datos) 
    
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

def subida_blob(upload_file_name,llista_final):
   # Creamos una conexión con un nuevo nombre de destino
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=upload_file_name)  
   
   # Cargamos los datos a un dataframe
   df = pd.DataFrame(llista_final[1:],columns = llista_final[0])
   data = df.to_csv(index=False,sep=";")
   # Los subimos a Blob Storage
   blob.upload_blob(data,overwrite=True)
   
   # Y también a SharePoint:
   
   #Creamos la conexión primero:
   ctx_auth = AuthenticationContext(siteurl) # should also be the siteurl
   ctx_auth.acquire_token_for_user(username, pwd)
   ctx = ClientContext(siteurl, ctx_auth) # make sure you auth to the siteurl.
   
   # Y subimos el fichero
   target_folder = ctx.web.get_folder_by_server_relative_url(relative_file_path)
   target_folder.upload_file(upload_file_name, data).execute_query()
   

def individual(llista,llista_final):
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
    return llista_final

def individual_SS(llista,llistafinal,provincia,llistaentitat):

    imptotal = ''
    anyactual = ''
    any1 = ''
    any2 = ''
    any3 = ''
    entitat = ''
    contador = 1
    for row in llista:
        if row[0] != '' and row[5].split(' ')[0] != 'TOTAL':
            organisme = row[0]
        if row[1] != '' and row[5].split(' ')[0] != 'TOTAL':
            programa = row[1]
        if row[2] != '' and row[5].split(' ')[0] != 'TOTAL':
            article = row[2]
        if row[3] != '' and row[5].split(' ')[0] != 'TOTAL':
            centretipus = row[3]
        if row[4] != '' and row[5].split(' ')[0] != 'TOTAL':
            numero = row[4]
        if row[5] != '' and row[5].split(' ')[0] != 'TOTAL':
            denominacio = row[5]
        if row[6] != '' and row[5].split(' ')[0] != 'TOTAL':
            inici = row[6]
        if row[7] != '' and row[5].split(' ')[0] != 'TOTAL':
            final = row[7]
        if row[8] != '' and row[5].split(' ')[0] != 'TOTAL':
            imptotal = row[8]
        if row[9] != '' and row[5].split(' ')[0] != 'TOTAL':
            anyactual = row[9]
        if row[10] != '' and row[5].split(' ')[0] != 'TOTAL':
            any1 = row[10]
        if row[11] != '' and row[5].split(' ')[0] != 'TOTAL':
            any2 = row[11]
        if row[12] != '' and row[5].split(' ')[0] != 'TOTAL':
            any3 = row[12]
        if row[5] != '' and row[5].split(' ')[0] == 'TOTAL' and row[5].split(' ')[1] == 'ENTIDAD....':
            toappendentitat = []
            toappendentitat.extend([provincia[:1] + str(contador), row[5][24:]])
            llistaentitat.append(toappendentitat)
            contador = contador + 1
    
        if row[5].split(' ')[0] != 'TOTAL':
            toappend = []
            toappend.extend([organisme, programa, article, centretipus, numero, denominacio, inici, final,imptotal, anyactual, any1, any2, any3, provincia, provincia[:1] + str(contador)])
            llistafinal.append(toappend)
    return llistafinal


def lista_sharepoint(prefix = None, suffix = None):
   ctx_auth = AuthenticationContext(siteurl) # should also be the siteurl
   ctx_auth.acquire_token_for_user(username, pwd)
   ctx = ClientContext(siteurl, ctx_auth) # make sure you auth to the siteurl.
   
   libraryRoot = ctx.web.get_folder_by_server_relative_path(relative_file_path_no_slash)
   
   files = libraryRoot.files
   ctx.load(files)
   ctx.execute_query()
   
   file_list = []
   
   for file in files:
      file_name = file.properties["Name"]
      if prefix is not None:
         if file_name[:len(prefix)] == prefix:
            file_list.append(file_name)
      else:
         if file_name[-len(suffix):] == suffix:
            file_list.append(file_name)         
            
   return file_list

def descarga_lista_sharepoint(lista_ficheros):
   ctx_auth = AuthenticationContext(siteurl) # should also be the siteurl
   ctx_auth.acquire_token_for_user(username, pwd)
   ctx = ClientContext(siteurl, ctx_auth) # make sure you auth to the siteurl.

   for fichero in lista_ficheros: 
      # Bajada y subida de fichero
      down_file_path = relative_file_path + fichero
      
      # Creamos una conexión con un nuevo nombre de destino
      blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName,blob_name=fichero)
      
      response = File.open_binary(ctx, down_file_path)
      blob.upload_blob(response.content, overwrite=True)

   

app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"


# Descargamos los ficheros del SharePoint
@app.route('/download', methods=['GET'])
def download_files():

   with open(os.path.join(app.root_path, 'lista_ficheros.lst'),'r') as f:
      lista_ficheros = f.read().splitlines()
    
   descarga_lista_sharepoint(lista_ficheros)
  
   return "Archivos descargados correctamente"





# Descargamos los ficheros del SharePoint
@app.route('/monthly_download', methods=['GET'])
def monthly_download_script():
   
   ctx_auth = AuthenticationContext(siteurl) # should also be the siteurl
   ctx_auth.acquire_token_for_user(username, pwd)
   ctx = ClientContext(siteurl, ctx_auth) # make sure you auth to the siteurl.
    
   currentMonth = datetime.now().strftime('%m')
   currentYear = datetime.now().year 
    
   fichero = str(currentYear) + "_" + currentMonth + "_LIA_FACT.xlsx" 

   # Bajada y subida de fichero
   down_file_path = relative_file_path + fichero
   
   # Creamos una conexión con un nuevo nombre de destino
   blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName,blob_name=fichero)
   
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
   flag_for = 0
   for row in csv_origen:
       # Empezamos a cargar desde el primer registro con la primera columna no nula
       if row[0] != '':
           flag_for = 1
       # Cargamos registros con longitud > 1 cuya segunda columna no esté vacia
       if flag_for == 1 and len(row) > 1:
           if row[1] != '':
              llista_origen.append(row)
              
   for row in csv_origen:
       llista_any.append(row)
   
   
   anyo = llista_any[3][1].split(' ')[2]
   
   # Creem una llista amb els ministeris - seccions
   for x,item in enumerate(llista_origen[0]):
      if x > 0 and item != '' and item != 'Total':
         llista_seccions.append(item)
   
   # Eliminem la primera fila
   llista_origen.pop(0)
   
   # Eliminem la primera columna
   for x in range(len(llista_origen)):
       llista_origen[x].pop(0)
   
   # Llista CCAA per treure-les
   llista_CCAA = ['PAIS VASCO', 'CATALUÑA', 'GALICIA', 'ANDALUCIA', 'ASTURIAS', 'CANTABRIA', 'LA RIOJA','REGION DE MURCIA', 'COMUNIDAD VALENCIANA', 'ARAGON', 'CASTILLA-LA MANCHA', 'CANARIAS', 'NAVARRA','EXTREMADURA', 'BALEARS', 'MADRID', 'CASTILLA Y LEON', 'NO REGIONALIZABLE','EXTRANJERO']
   
   llista_sense_CCAA = []
   

   for row in llista_origen:
       if row[0] not in llista_CCAA:
           llista_sense_CCAA.append(row)
   
   # Ajuntem les llistes per tenir tota la info
   
   llista_sense_CCAA.insert(0, llista_seccions)
   
   llista = llista_sense_CCAA
   
   # Creem un procés que iteri cada quadrat, n'extregui la info de la seva respectiva primera fila i columna i les posi en una llista de 3.
   
   llista_final = []
   
   for x in range(1,len(llista[0])):
       for y in range(1, len(llista)):
           llista_tupla = [llista[0][x], llista[y][0], llista[y][x]]
           llista_final.append(llista_tupla)
   
   capcelera = ['MINISTERI', 'COMUNIDAD AUTONOMA', 'COST TOTAL']
   llista_final.insert(0, capcelera)
   
   upload_file = anyo + '_PRES_FACT_AGR_CCAA_MIN_EST_OOAA_RE.csv'
   subida_blob(upload_file,llista_final)
   
   return 'Blob CCAA_Ministeris subido'



# 1.1
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\1. Estado, Organismos\Detalle
# Aquí determinamos el metodo GET de la URL /Estado_org
@app.route('/Estado_org/<int:Estado_year>', methods=['GET'])
def Estado_org_script(Estado_year: int):
    
    #Nos quedamos con los últimos digitos del año
    year_YY = Estado_year - 2000
    

    #Descargamos los ficheros que empiezan por N_YY
    prefix = 'N_'+str(year_YY)
    lista_ficheros = lista_sharepoint(prefix)
    descarga_lista_sharepoint(lista_ficheros)
    
    llista_final = []
    for file in lista_ficheros:
       fitxer_content = descarga_blob(file)
       llista_final = individual(fitxer_content,llista_final)
    
    #llista_108 = descarga_blob('N_22_E_V_2_R_1_202_1_108_1.CSV')
    #llista_114 = descarga_blob('N_22_E_V_2_R_1_202_1_114_1.CSV')
    #llista_115 = descarga_blob('N_22_E_V_2_R_1_202_1_115_1.CSV')
    #llista_116 = descarga_blob('N_22_E_V_2_R_1_202_1_116_1.CSV')
    #llista_117 = descarga_blob('N_22_E_V_2_R_1_202_1_117_1.CSV')
    #llista_119 = descarga_blob('N_22_E_V_2_R_1_202_1_119_1.CSV')
    #llista_120 = descarga_blob('N_22_E_V_2_R_1_202_1_120_1.CSV')
    #llista_123 = descarga_blob('N_22_E_V_2_R_1_202_1_123_1.CSV')
    #llista_124 = descarga_blob('N_22_E_V_2_R_1_202_1_124_1.CSV')
    #llista_128 = descarga_blob('N_22_E_V_2_R_1_202_1_128_1.CSV')
 
    #llista_final = individual(llista_108,llista_final)
    #llista_final = individual(llista_114,llista_final)
    #llista_final = individual(llista_115,llista_final)
    #llista_final = individual(llista_116,llista_final)
    #llista_final = individual(llista_117,llista_final)
    #llista_final = individual(llista_119,llista_final)
    #llista_final = individual(llista_120,llista_final)
    #llista_final = individual(llista_123,llista_final)
    #llista_final = individual(llista_124,llista_final)
    #llista_final = individual(llista_128,llista_final)
    #anyo = llista_108[5][1].split(' ')[2]
    
    capcelera = ['ID_MINISTERI','DESC_MINISTERI' ,'COMUNITAT_AUTONOMA', 'CODI_CENTRE','ID_PROGRAMA', 'ID_ARTICLE','DESC_CENTRE','ID_PROJECTE', 'NOM_PROJECTE','ANY_INICI', 'ANY_FI', 'PROVINCIA', 'TIPUS', 'COST_TOTAL', 'ANY_ANTERIOR', 'ANY_ACTUAL','ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    
    llista_final.insert(0, capcelera)

    upload_file = str(Estado_year) + '_PRES_FACT_DET_EST_OOAA_RE.csv'
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

    anyo = ''
    tipus_inversio = ''

    for x,row in enumerate(csv_reader_estado):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                anyo = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                estado.append([row[0], row[1], anyo, tipus_inversio])
    
    # Segon fitxer
    csv_reader_ooaa = descarga_blob('OOAA.CSV')
    anyo = ''
    tipus_inversio = ''
    for x,row in enumerate(csv_reader_ooaa):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                anyo = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                ooaa.append([row[0], row[1], anyo, tipus_inversio])
    
    # Tercer fitxer
    csv_reader_restoent = descarga_blob('RESTOENT.CSV')
    anyo = ''
    tipus_inversio = ''
    
    for x,row in enumerate(csv_reader_restoent):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                anyo = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                restoent.append([row[0], row[1], anyo, tipus_inversio])
                
    # Quart fitxer             
    csv_reader_ss_ss = descarga_blob('SS_SS.CSV')
    anyo = ''
    tipus_inversio = ''
    for x,row in enumerate(csv_reader_ss_ss):
        if len(row) > 0:
            if x == 1:
                tipus_inversio = (row[1].split(" "))[3]
            if x == 2:
                anyo = (row[1].split(" "))[2]
            elif x >= 8 and row[0] != 'TOTAL' and row[0] != '':
                ss_ss.append([row[0], row[1], anyo, tipus_inversio])
            
    
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
    
    
    upload_file = str(anyo)+'_PRES_FACT_AGR_RESUMEN_INVERSIONS.csv'
    subida_blob(upload_file,llista_final)
    
    return 'Blob Resumen Inv subido'





# 1.3
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\3. Sector publico administrativo\Agrupación
# Aquí determinamos el metodo GET de la URL /Agr_SP_Admin
@app.route('/Agr_SP_Admin', methods=['GET'])
def Agr_SP_Admin_script():

    # creem les llistes buides i descarreguem fitxer
    llista= descarga_blob('Resum_SP_Admin.CSV')
    llista_final=[]

    # Afegim la capcelera a la llista
    header=['COMUNITAT AUTONOMA', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    llista_final.append(header)
    
    # Afegim els registres interessants a la llista
    flag_com = 0
    for i,row in enumerate(llista):
        if "PRESUPUESTO" in row[0]:
            split_row = row[0].split(' ')       
        # Paramos en la fila que contiene las palabras NO REGIONALIZABLE en la primera columna
        if "NO REGIONALIZABLE" in row[0]:
            flag_com = 0
        # Eliminamos la columna 6 que aparece vacia en el CSV
        if flag_com == 1 and row[0] != '':
            llista_final.append(row[:6])
        #Empezamos a cargar despues de la fila que contiene las palabras "COMUNIDAD AUTÓNOMA"
        if "COMUNIDAD AUTÓNOMA" in row[0]:
            flag_com = 1
    
    for i,item in enumerate(split_row):
        if item == 'PRESUPUESTO':
            index_presu = i
    anyo = split_row[index_presu+1]
    
    upload_file = anyo + '_PRES_FACT_AGR_CCAA_SP_ADMIN.csv'
    subida_blob(upload_file,llista_final)
    
    return 'Blob Agr_SP_Admin subido'
    





# 1.3
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\3. Sector publico administrativo\Detalle
# Aquí determinamos el metodo GET de la URL /SP_Admin
@app.route('/SP_Admin', methods=['GET'])
def SP_Admin_script():

   llista_origen = descarga_blob("Detall_SP_Admin.CSV")

   llista_final = []
   
   comunitat = "CATALUÑA"
   provincia = ""
   entidad = ""
   
   for row in llista_origen:
       if len(row) != 0 and "PROVINCIA" in row[0]:
           provincia = row[0].split(" ")[-1]
       if len(row) > 2 and "ENTIDAD" in row[2]:
           aux = row[2].split(":")
           if len(aux) > 1:
               entidad = aux[1]
       if len(row) != 0 and row[0].isdigit():
           toappend = []
           toappend.extend([comunitat, provincia, entidad, row[0]])
           toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
           llista_final.append(toappend)
   
   capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI','TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
   
   llista_final.insert(0, capcelera)
   
   # Buscamos la palabra PARA
   # Texto de ejemplo en esa fila: " ANEXO DE INVERSIONES REALES PARA 2023 Y PROGRAMACIÓN PLURIANUAL ANUALIZADA"
   split_row_0 = llista_origen[0][0].split(' ')
   for x,item in enumerate(split_row_0):
      if item == 'PARA':
         index_any = x+1
   anyo = split_row_0[index_any]
    
   upload_file	= anyo + "_PRES_FACT_DET_SP_ADMIN.csv"
   subida_blob(upload_file,llista_final)
      
   return 'Blob SP_Admin subido'






# 1.4
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\4. Sector publico empresarial\Agrupación
# Aquí determinamos el metodo GET de la URL /Agr_SP_Empresarial
@app.route('/Agr_SP_Empresarial', methods=['GET'])
def Agr_SP_Empresarial_script():


    # creem les llistes buides i descarreguem fitxers
    llista= descarga_blob('Resum_SP_Emp.CSV')
    llistafinal=[]

    
    # Afegim els registres interessants a la llista
    flag_comunidad = 0
    for item in llista:
        # Cuando encontremos la palabra "EXTRANJERO" paramos
        if "EXTRANJERO" in item[0]:
            flag_comunidad = 0       
            
        if flag_comunidad == 1 and item[0] != '':
            llistafinal.append(item[:6])
            
        #Empezamos a cargar después de la fila que tenga las palabras "COMUNIDAD AUTÓNOMA"
        if "COMUNIDAD AUTÓNOMA" in item[0]:
            flag_comunidad = 1

    
    # Afegim la capcelera a la llista
    header=['COMUNITATS AUTONOMES', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    llistafinal.insert(0, header)
    
    # En la primera fila buscamos la palabra "PRESUPUESTO" y cogemos la siguiente palabra, que contiene el año
    row_split = llista[0][0].split(' ')
    for i,item in enumerate(row_split):
        if item == 'PRESUPUESTO':
            index_presu = i
    anyo = row_split[index_presu+1]
    
    upload_file	= anyo + '_PRES_FACT_AGR_CCAA_SP_EMPR.csv'
    subida_blob(upload_file,llistafinal)
    
    return 'Blob Agr SP Empresarial'

    

# 1.4
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\4. Sector publico empresarial\Detalle
# Aquí determinamos el metodo GET de la URL /Detall_SP_Empresarial
@app.route('/Detall_SP_Empresarial', methods=['GET'])
def Detall_SP_Empresarial_script():

    
    llista_origen = descarga_blob('Detall_SP_Emp.CSV')

    llista_final = []
    
    comunitat = "CATALUÑA"
    provincia = ''
    entidad = ''
    
    for row in llista_origen:
        if len(row) != 0 and "PROVINCIA" in row[0]:
            provincia = row[0].split(" ")[-1]
        if len(row) > 2 and "ENTIDAD" in row[2]:
            aux = row[2].split(":")
            if len(aux) > 1:
                entidad = aux[1]
        if len(row) != 0 and row[0].isdigit():
            toappend = []
            toappend.extend([comunitat, provincia, entidad, row[0]])
            toappend.extend(list(row[i] for i in [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
            llista_final.append(toappend)
    
    capcelera = ['COMUNITAT_AUTONOMA', 'PROVINCIA', 'ENTITAT', 'CODI PROJECTE', 'DENOMINACIO', 'COST TOTAL', 'INICI', 'FI','TIPUS', 'ANY_ANTERIOR', 'ANY_ACTUAL', 'ANY_ACTUAL+1', 'ANY_ACTUAL+2', 'ANY_ACTUAL+3']
    
    llista_final.insert(0, capcelera)
    
    # En la primera fila buscamos la palabra "ESTADO" y cogemos la siguiente palabra, que contiene el año
    row_split = llista_origen[0][0].split(' ')
    for i,item in enumerate(row_split):
        if item == 'ESTADO':
            index_estado = i
    anyo = row_split[index_estado+1]
    
    upload_file	= anyo + '_PRES_FACT_DET_SP_EMPR.csv'
    subida_blob(upload_file,llista_final)

    return 'Blob Detall SP Empresarial'        
    


        
    
# 1.5
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\1.Pressupost\5. Seguretat Social
# Aquí determinamos el metodo GET de la URL /SS
@app.route('/SS/<int:SS_year>', methods=['GET'])
def SS_script(SS_year: int):

    #lastYear = datetime.now().year
    llistafinal = []
    llistaentitat = []
    llista_BCN = descarga_blob('SS_BCN.CSV')
    llista_TGN = descarga_blob('SS_TGN.CSV')
    llista_LLEIDA = descarga_blob('SS_LLEIDA.CSV')
    llista_GIR = descarga_blob('SS_GIR.CSV')
    
    header = ['Organisme', 'Programa', 'Article', 'Centre Tipus', 'Número', 'Denominació', 'Inici', 'Final','Import Total', 'ANY', 'ANY+1', 'ANY+2', 'ANY+3', 'Província', 'ID Entitat']
    
    headerentitat = ['ID Entitat', 'Entitat']
    
    
    llistaentitat.insert(0, headerentitat)
    llistafinal.insert(0, header)
    llistafinal = individual_SS(llista_BCN,llistafinal,'BARCELONA',llistaentitat)
    llistafinal = individual_SS(llista_TGN,llistafinal,'TARRAGONA',llistaentitat)
    llistafinal = individual_SS(llista_LLEIDA,llistafinal,'LLEIDA',llistaentitat)
    llistafinal = individual_SS(llista_GIR,llistafinal,'GIRONA',llistaentitat)
    
    upload_file	= str(SS_year) + "_PRES_FACT_DET_SEGURETAT_SOCIAL.csv"
    subida_blob(upload_file,llistafinal)

    upload_file	= "DIM_DET_SEGURETAT_SOCIAL_ENTITATS.csv"
    subida_blob(upload_file,llistaentitat)    

    return 'Blob SS subido'


        
# 2
# Python original: 
# G:\Unidades compartidas\Sector Públic BCN\01. Generalitat de Catalunya\07. PDA\01. Projectes\202210_GENE UTE SPD - QdC Seguiment Inversions estat\07. Document tècnic\Python\2. Execució Pressupostària
# Aquí determinamos el metodo GET de la URL /Pressupostaria
@app.route('/Pressupostaria', methods=['GET'])
def Pressupostaria_script():



    # specify the file path
    file_path = 'ORIGEN.xlsx'
    
    # Posicions de les pàgines que ens interessen
    posicions = [0, 2, 24, 26, 48, 50, 67, 69]
    
    # read the excel file
    df = descarga_excel(file_path,posicions)
    
    data = []
    for x in posicions:
        element = df[x]
        data.append(element)
    
    CCAA0 = data[0]
    cat1 = data[1]
    CCAA2 = data[2]
    cat3 = data[3]
    CCAA4 = data[4]
    cat5 = data[5]
    CCAA6 = data[6]
    cat7 = data[7]
    
    
    
    # Transformem el diccionari en llistes individuals
    
    lCCAA0 = CCAA0.values.tolist()
    lcat1 = cat1.values.tolist()
    lCCAA2 = CCAA2.values.tolist()
    lcat3 = cat3.values.tolist()
    lCCAA4 = CCAA4.values.tolist()
    lcat5 = cat5.values.tolist()
    lCCAA6 = CCAA6.values.tolist()
    lcat7 = cat7.values.tolist()
    
    anyo = lcat1[1][0].split(' ')[16]
    
    # Retallem les fileres que no ens interessen per cada llista
    
    ################# Per agrupacions segons Comunitats Autònomes ###################
    
    # Per CCAA0
    
    llista_CCAA0 = []
    capcelera_CCAA = ['IDCCAA', 'Crèdit Inicial', 'Obligacions Reconegudes', '%']
    capcelera_CCAA2 = ['IDCCAA', 'Inversió Inicial', 'Inversió Real', '%']
    x = 7
    
    for row in lCCAA0:
        if x > 6:
            if lCCAA0[x][0].split(' ')[0] == 'Total':
                break
            else:
                ID_CCAA = lCCAA0[x][0].split(' ')[0]
                Credit_Ini = lCCAA0[x][1]
                Credit_Fi = lCCAA0[x][2]
                Perc = 100 * lCCAA0[x][3]
                fila = [str(ID_CCAA), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CCAA0.append(fila)
        x = x+1
    
    llista_CCAA0.insert(0, capcelera_CCAA)
    
    upload_file	= anyo + "_EXEC_FACT_AGR_CCAA_AGE.csv"
    subida_blob(upload_file,llista_CCAA0)

    
    # Per CCAA 2
    llista_CCAA2 = []
    x = 7
    
    for row in lCCAA2:
        if x > 6:
            if lCCAA2[x][0].split(' ')[0] == 'Total':
                break
            else:
                ID_CCAA = lCCAA2[x][0].split(' ')[0]
                Credit_Ini = lCCAA2[x][1]
                Credit_Fi = lCCAA2[x][2]
                Perc = 100 * lCCAA2[x][3]
                fila = [str(ID_CCAA), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CCAA2.append(fila)
        x = x+1
    
    llista_CCAA2.insert(0, capcelera_CCAA)

    upload_file	= anyo + "_EXEC_FACT_AGR_CCAA_OOAA_RE.csv"
    subida_blob(upload_file,llista_CCAA2)
    
    # Per CCAA 4
    llista_CCAA4 = []
    x = 7
    
    for row in lCCAA4:
        if x > 6:
            if lCCAA4[x][0].split(' ')[0] == 'Total':
                break
            else:
                ID_CCAA = lCCAA4[x][0].split(' ')[0]
                Credit_Ini = lCCAA4[x][1]
                Credit_Fi = lCCAA4[x][2]
                Perc = 100 * lCCAA4[x][3]
                fila = [str(ID_CCAA), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CCAA4.append(fila)
        x = x+1
    
    llista_CCAA4.insert(0, capcelera_CCAA2)

    upload_file	= anyo + "_EXEC_FACT_AGR_CCAA_SP_ADMIN.csv"
    subida_blob(upload_file,llista_CCAA4)
        
    
    # Per CCAA 6
    llista_CCAA6 = []
    x = 7
    
    for row in lCCAA6:
        if x > 6:
            if lCCAA6[x][0].split(' ')[0] == 'Total':
                break
            else:
                ID_CCAA = lCCAA6[x][0].split(' ')[0]
                Credit_Ini = lCCAA6[x][1]
                Credit_Fi = lCCAA6[x][2]
                Perc = 100 * lCCAA6[x][3]
                fila = [str(ID_CCAA), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CCAA6.append(fila)
        x = x+1
    
    llista_CCAA6.insert(0, capcelera_CCAA2)

    upload_file	= anyo + "_EXEC_FACT_AGR_CCAA_SP_EMPR.csv"
    subida_blob(upload_file,llista_CCAA6)
   
    
    ######################### Per Catalunya ################################
    
    llista_CAT1 = []
    capcelera_CAT1 = ['Codi Secció', 'Crèdit Inicial', 'Obligacions Reconegudes', '%']
    x = 7
    
    for row in lcat1:
        if x > 6:
    
            if lcat1[x][0].split(' ')[0] == 'Total':
                break
            else:
                ID_SECCIO = lcat1[x][0]
                Credit_Ini = lcat1[x][2]
                Credit_Fi = lcat1[x][3]
                Perc = 100 * lcat1[x][4]
                fila = [str(ID_SECCIO), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CAT1.append(fila)
        x = x+1
    
    llista_CAT1.insert(0, capcelera_CAT1)

    upload_file	= anyo + "_EXEC_FACT_DET_AGE.csv"
    subida_blob(upload_file,llista_CAT1)

    
    # Per CAT 3
    
    llista_CAT3 = []
    #capcelera_CAT3 = ['Codi Presupostari Organisme', 'Denominació', 'Crèdit Inicial', 'Obligacions Reconegudes', '%']
    capcelera_CAT3 = ['Codi Secció', 'Crèdit Inicial', 'Obligacions Reconegudes', '%']
    x = 7
    
    for row in lcat3:
        if x > 6:
            if lcat3[x][0] == 'Totales':
                break
            elif lcat3[x][1] > 0:
                ID_SECCIO = lcat3[x][1]
                #denominacio = lcat3[x][2]
                Credit_Ini = lcat3[x][3]
                obligacions = lcat3[x][4]
                Perc = 100 * lcat3[x][5]
                fila = [str(ID_SECCIO), str(Credit_Ini), str(obligacions), str(Perc)]
                llista_CAT3.append(fila)
        x = x+1
    
    llista_CAT3.insert(0, capcelera_CAT3)
    
    upload_file	= anyo + "_EXEC_FACT_DET_OOAA_RE.csv"
    subida_blob(upload_file,llista_CAT3)

    
    # Per CAT 5
    llista_CAT5 = []
    capcelera_CAT5 = ['Entitat', 'Inversio Inicial', 'Inversio Real', '%']
    x = 7
    
    for row in lcat5:
        if x > 6:
            if lcat5[x][0] == 'Totales':
                break
            else:
                ID_SECCIO = lcat5[x][0]
                Credit_Ini = lcat5[x][1]
                Credit_Fi = lcat5[x][2]
                Perc = 100 * lcat5[x][3]
                fila = [str(ID_SECCIO), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CAT5.append(fila)
        x = x+1
    
    llista_CAT5.insert(0, capcelera_CAT5)

    upload_file	= anyo + "_EXEC_FACT_DET_SP_ADMIN.csv"
    subida_blob(upload_file,llista_CAT5)
    
    # Per CAT 7
    llista_CAT7 = []
    capcelera_CAT7 = ['Entitat', 'Inversió Inicial', 'Inversió Real', '%']
    x = 7
    
    for row in lcat7:
        if x > 6:
            if lcat7[x][0] == 'Totales':
                break
            else:
                ID_SECCIO = lcat7[x][0]
                Credit_Ini = lcat7[x][1]
                Credit_Fi = lcat7[x][2]
                Perc = 100 * lcat7[x][3]
                fila = [str(ID_SECCIO), str(Credit_Ini), str(Credit_Fi), str(Perc)]
                llista_CAT7.append(fila)
        x = x+1
    
    llista_CAT7.insert(0, capcelera_CAT7)
    
    upload_file	= anyo + "_EXEC_FACT_DET_SP_EMPR.csv"
    subida_blob(upload_file,llista_CAT7)

    return 'Blobs pressupostaries subidas'




# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

