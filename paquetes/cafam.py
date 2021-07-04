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
from selenium.webdriver.common.keys import Keys
import urllib.request
from contextlib import closing
from datetime import datetime as dt
import pandas as pd
import json
import random

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.drogueriascafam.com.co'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
    navegador.get(url_principal)
    
    menu = BeautifulSoup(
        navegador.page_source,
        'html.parser'
    ).find_all('div', {'class': 'iqitmegamenu-wrapper col-xs-12 cbp-hor-width-1 clearfix'})[0].find_all('li', {'class': 'cbp-hrmenu-tab'})
    
    cat_urls = [x.get_attribute_list('href')[0] for x in menu[0].find_all('a') if len(x.get_attribute_list('href')[0].split('/'))==4]

    # Guarda la lista de URLs en un archivo csv
    with open('./web_scraping/data/cat_urls_cafam.csv', 'w+') as f:
        f.write('\n'.join(cat_urls))

    print(f'Número de urls de categorias: {len(cat_urls)}')
    cat_urls[:10]

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.drogueriascafam.com.co'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

# Leer el archivo con las url de categorias
with open('./web_scraping/data/cat_urls_cafam.csv', 'r') as f:
    cat_urls = f.read()
cat_urls = cat_urls.split('\n')

def carga_producto_postgres(fila):
    db = conn2('fsalinas', False)
    sql = f'''
    INSERT INTO web_scraping.drog_cafam (url_producto, fecha_scraping, hora_scraping, titulo, marca_producto, precio_tachado, precio_original)
    SELECT *
    FROM (values{fila}) as s(url_producto, fecha_scraping, hora_scraping, titulo, marca_producto, precio_tachado, precio_original)
    '''
    print(str(RunDML(sql, db)[0])[:100], end='\r')
    db.close()

def cambia_pagina(navegador):
    time.sleep(1)
    try:
        paginacion = navegador.find_elements_by_id('pagination')[0]
        paginacion.find_element_by_class_name('pagination_next').click()
        return True
    except:
        return False

def obtiene_productos(prod):

    fecha_scraping, hora_scraping = dt.now().strftime('%Y-%m-%d'), dt.now().strftime('%H:%M:%S')
    cols = ['url_producto', 'fecha_scraping', 'hora_scraping', 'titulo', 'marca_producto', 'precio_tachado', 'precio_original']
    
    try:
        marca_producto = prod.find('span',{'class':'desc-grid'}).text.strip()
    except:
        marca_producto = 'No conseguida'

    try:
        titulo = prod.find('span',{'class':'grid-name'}).text.strip()
    except:
        titulo = 'No conseguida'

    try:
        url_producto = prod.find('a').get_attribute_list('href')[0]
    except:
        url_producto = 'No conseguida'

    try:
        precio_tachado = int(prod.find('span',{'class':'old-price product-price'}).text.strip().replace('$ ','').replace(',',''))
    except:
        precio_tachado = 0

    try:
        precio_original = int(prod.find('span',{'class':'price product-price'}).text.strip().replace('$ ','').replace(',',''))
    except:
        precio_original = 0

    df_productos = pd.DataFrame.from_dict(data = {
        'url_producto': url_producto,
        'fecha_scraping': fecha_scraping,
        'hora_scraping': hora_scraping,
        'titulo': titulo,
        'marca_producto': marca_producto,
        'precio_tachado': precio_tachado,
        'precio_original': precio_original
    }, orient = 'index').T
    
    carga_producto_postgres(tuple(df_productos.iloc[0,:]))
    
    return df_productos

def procesa_url_cat(navegador):
    ldfs = []
    while True:
        last_url = navegador.current_url
        while True:
            if cambia_pagina(navegador): break

        lista_productos = BeautifulSoup(navegador.page_source, 'html.parser').find('div',{'class':'list-wrapper'}).find_all('li')

        lista_df_productos = []
        for prod in lista_productos:
            lista_df_productos.append(obtiene_productos(prod))

        ldfs.append(pd.concat(lista_df_productos, ignore_index=True))

        if last_url == navegador.current_url:
            break
    df = pd.concat(ldfs, ignore_index=True).drop_duplicates(subset='url_producto').drop_duplicates(subset='url_producto')
    return df

with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
    for url in cat_urls:
        print(f'{dt.now().strftime("%H:%M:%S")}: Procesando {url}')
        navegador.get(url)
        df = procesa_url_cat(navegador)