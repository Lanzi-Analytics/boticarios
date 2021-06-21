import time
import os
os.chdir(r'C:\Users\Fsalinas\Documents\GitHub\boticarios')
time.sleep(2)
os.chdir('./paquetes')
from connpostgres import conn2
from runSQL import RunDML, RunDDL
time.sleep(2)
os.chdir(r'C:\Users\Fsalinas\Documents\GitHub\boticarios')

from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import urllib.request
from contextlib import closing
from datetime import datetime as dt
import pandas as pd
import json
import random

def carga_producto_postgres(dc_prod):
    db = conn2('fsalinas', False)
    sql = f'''
    INSERT INTO web_scraping.drog_colsubsidio (url_producto, fecha_hora_scraping, breadcumb, titulo, nombre_imagen, presentacion, precio, descripcion, atributos)
    SELECT *
    FROM (values{
        tuple([str(x).replace("'","") for x in pd.DataFrame.from_dict(dc_prod, orient='index').T.values.tolist()[0]])
        }) as s(url_producto, fecha_hora_scraping, breadcumb, titulo, nombre_imagen, presentacion, precio, descripcion, atributos)
    '''
    RunDML(sql, db)[0]
    db.close()

def obtiene_data_producto_de_cat(navegador):
    fecha_scraping = dt.now().strftime('%Y-%m-%d')
    hora_scraping = dt.now().strftime('%H:%M:%S')
    soup_productos = BeautifulSoup(navegador.page_source, 'html.parser').find_all('div', {'class':'product-Vitrina-masVendidos js-productVitrineShowcase WishlistModule rendered'})
    
    lista_data_productos = []
    for prod in soup_productos:
        url_producto = prod.find_all('a')[0].get_attribute_list('href')[0]
        titulo = prod.find('p', {'class': 'dataproducto-nameProduct'}).text
        
        try:
            presentacion = prod.find('div', {'class': 'dataproducto-info dataproducto-Presentacion'}).text.strip()
        except:
            presentacion = 'N/A'
        
        try:
            precio_tachado = float(prod.find('div', {'class': 'precioTachadoVitrina'}).text.replace('Antes: $','').replace('.','').replace(',','.'))
        except:
            precio_tachado = 0
        
        try:
            precio_final = float(prod.find('p', {'class': 'dataproducto-bestPrice'}).text.replace('$','').replace('.','').replace(',','.'))
        except:
            precio_final = 0
        
        lista_data_productos += [{
            'url_producto': url_producto,
            'fecha_scraping': fecha_scraping,
            'hora_scraping': hora_scraping,
            'titulo': titulo,
            'presentacion': presentacion,
            'precio_tachado': precio_tachado,
            'precio_final': precio_final
        }]
    
    return pd.concat([pd.DataFrame.from_dict(x, orient='index').T for x in lista_data_productos], axis=0, ignore_index=True)

def carga_producto_postgres2(df_prods):
    db = conn2('fsalinas', False)
    sql = f'''
    INSERT INTO web_scraping.drog_colsubsidio2 (url_producto, fecha_scraping, hora_scraping, titulo, presentacion, precio_tachado, precio_final)
    SELECT *
    FROM (values{str([tuple(x) for x in df_prods.values.tolist()]).replace("[","").replace("]","")}) as s(url_producto, fecha_scraping, hora_scraping, titulo, presentacion, precio_tachado, precio_final)
    '''
    resultado = str(RunDML(sql, db))
    db.close()
    return resultado

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.drogueriascolsubsidio.com'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

# Crea el objeto del navegador con el que se realizará la interacción
with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:

    # Navega a la URL principal donde se extraerán las URLs de categorias y subcategorias de producto
    navegador.get(url_principal)
    soup = BeautifulSoup(navegador.page_source, 'html.parser')

    # Obtiene una lista de las etiquetas "ul" asociadas a las categorias de producto
    # Fuente: investigación en las particularidades de la construcción del sitio web, es suceptible a fallos en caso de que
    # la estructura (etiquetas) del sitio web cambie.
    lista_categorias = soup.find_all('ul',{'class':'categoria-container'})
    dc_cat_url = {cat.find_all('a')[1].text.lower().replace('ver ', ''):cat for x, cat in enumerate(lista_categorias)}

    #Extrae la lista de URLs de las etiquetas "ul"
    lista_urls = []
    for cat in dc_cat_url.keys():
        lista_urls += [x.get_attribute_list('href')[0] for x in dc_cat_url[cat].find_all('a')]

    lista_urls = list(dict.fromkeys(lista_urls))

# Guarda la lista de URLs en un archivo csv
with open('./web_scraping/data/cat_urls.csv', 'w+') as f:
    f.write('\n'.join(lista_urls))

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.drogueriascolsubsidio.com'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

# Leer el archivo con las url de categorias
with open('./web_scraping/data/cat_urls.csv', 'r') as f:
    cat_urls = f.read()
cat_urls = cat_urls.split('\n')

# Función para obtener todas las url de producto ubicadas en una url
def obtiene_url_productos(navegador):
    soup = BeautifulSoup(navegador.page_source, 'html.parser')
    urls_prod = [x.get_attribute_list('href')[0] for x in soup.find_all('a') if x.get_attribute_list('href')[0]!=None and x.get_attribute_list('href')[0][-2:].lower()=='/p']
    urls_prod = [x.replace(url_principal, '') for x in urls_prod]
    return list(dict.fromkeys(urls_prod))

def obtiene_urls_producto(navegador, url, tiempo_espera_scroll=2):
    # Navega a cada URL de categoria o subcategoria donde se extraerán las URLs de producto
    navegador.get(url)

    # Obtener el tamaño de la pagina cargada
    ultima_altura = navegador.execute_script("return document.body.scrollHeight")

    lista_urls_producto, scroll_nro = [], 1
    while True:
        print(f'{dt.now().strftime("%H:%M:%S")} Scroll nro: {scroll_nro:,.0f}', end = '\r')
        # Scroll hasta el final de la pagina
        navegador.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # captura las urls de la pagina hasta donde está cargada
        lista_urls_producto += obtiene_url_productos(navegador)

        # esperar a que la pagina cargue
        time.sleep(tiempo_espera_scroll)

        # Calcula la nueva altura de la pagina despues de hacer scroll y lo compara con la latura anterior
        nueva_altura = navegador.execute_script("return document.body.scrollHeight")
        if nueva_altura == ultima_altura:
            break
        ultima_altura = nueva_altura
        scroll_nro += 1
    
    df_prods = obtiene_data_producto_de_cat(navegador)
    print('Resultado de carga:\n' + '\n'.join([carga_producto_postgres2(df_prods.iloc[x:x+1, :])[:20] for x in df_prods.index]))

    lista_urls_producto = list(dict.fromkeys(lista_urls_producto))
    return lista_urls_producto

# Crea el objeto del navegador con el que se realizará la interacción
# Duración aproximada: 30 minutos
with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
    lista_urls_productos = []
    for cat in cat_urls:
        print(f'{dt.now().strftime("%H:%M:%S")} Procesando: {cat}')
        lp = obtiene_urls_producto(navegador, url_principal + cat, 2)
        print(f'{dt.now().strftime("%H:%M:%S")} Numero de urls de producto conseguidas:{len(lp):,.0f}')
        lista_urls_productos += lp

lista_urls_productos = list(dict.fromkeys(lista_urls_productos))
print(f'{dt.now().strftime("%H:%M:%S")} Numero de urls de producto totales conseguidas:{len(lista_urls_productos):,.0f}')
print('Primeras 10 urls de la lista completa:')
lista_urls_productos[:10]