import time
import os
os.chdir(r'C:\Users\carlo\Documents\GitHub\boticarios')
time.sleep(2)
os.chdir('./paquetes')
from connpostgres import conn2
#from runSQL import RunDML, RunDDL
time.sleep(2)
os.chdir(r'C:\Users\carlo\Documents\GitHub\boticarios')

from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
import urllib.request
from contextlib import closing
from datetime import datetime as dt
import pandas as pd
import json
import random

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.larebajavirtual.com/catalogo/categoria/categoria/2700' # https://www.larebajavirtual.com https://www.larebajavirtual.com/catalogo/categoria/categoria/2700

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

#patrones_link = ['#navigation-item-', 'ofertas', 'medicamentos', 'cuidado-del-bebe', 'belleza', 'nutricion', 'cuidado-personal',
#                 'rehabilitacion-y-monitoreo', 'bienestar', 'alimentos']
#
#def filtra_links(X):
#   return (sum([patron in X for patron in patrones_link])>=1)&('https://www.larebajavirtual.com' not in X)

with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
    navegador.get(url_principal)
    
    cat_urls = [x.get_attribute_list('href')[0].replace(url_principal, '') for x in BeautifulSoup(navegador.page_source, 'html.parser').find_all('a')]
#    cat_urls = [x for x in cat_urls if filtra_links(x)]
#    cat_urls = ['/' + x if x[:1]!='/' else x for x in cat_urls]

    # Guarda la lista de URLs en un archivo csv
#    with open('./web_scraping/data/cat_urls_larebaja.csv', 'w+') as f:
#        f.write('\n'.join(cat_urls))

#    print(f'Número de urls de categorias: {len(cat_urls)}')
#    cat_urls[:10]

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.larebajavirtual.com/catalogo/categoria/categoria/2700'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

# Leer el archivo con las url de categorias
#with open('./web_scraping/data/cat_urls_larebaja.csv', 'r') as f:
#    cat_urls = f.read()
#cat_urls = cat_urls.split('\n')

def obtiene_productos(navegador):
    productos = BeautifulSoup(navegador.page_source, 'html.parser').find_all('div', {'class': 'col-12 col-lg-4'})

    df_productos, fecha_scraping, hora_scraping = [], dt.now().strftime('%Y-%m-%d'), dt.now().strftime('%H:%M:%S')
    cols = ['url_producto', 'fecha_scraping', 'hora_scraping', 'titulo', 'marca_producto', 'nota_envio', 'precio_oferta', 'precio_original']
    for producto in productos:
        try:
            marca_producto = producto.find_all('a', {'class': 'product-brand text-uppercase m-0'})[0].text.strip()
        except:
            marca_producto = 'No conseguida'
        
        try:
            titulo = producto.find_all('a', {'class': 'link'})[0].text.strip()
        except:
            titulo = 'No conseguida'
        
        try:
            url_producto = producto.find_all('a', {'class': 'link'})[0].get_attribute_list('href')[0]
        except:
            url_producto = 'No conseguida'
        
        try:
            precio_oferta = float(''.join([x for x in producto.find_all('div', {'class': 'large-price d-flex'})[0].text.replace('\n', '').strip() if x.isdigit()]))
        except:
            precio_oferta = 0
        
        try:
            precio_original = float(''.join([x for x in producto.find_all('span', {'class': 'price-original'})[0].text.replace('\n', '').strip() if x.isdigit()]))
        except:
            precio_original = 0
        
        try:
            nota_envio = producto.find_all('div', {'class': 'track-shipping'})[0].text.replace('\n', '').strip().replace('Sólo', 'Sólo ').replace('DomicilioDomicilio', 'Domicilio')
        except:
            nota_envio = 'No conseguida'

        df_productos += [pd.DataFrame.from_dict(data = {
            'url_producto': url_producto,
            'fecha_scraping': fecha_scraping,
            'hora_scraping': hora_scraping,
            'titulo': titulo,
            'marca_producto': marca_producto,
            'nota_envio': nota_envio,
            'precio_oferta': precio_oferta,
            'precio_original': precio_original
        }, orient = 'index').T]
    
    df_productos = pd.concat(df_productos, axis=0, ignore_index=True).drop_duplicates(subset='url_producto')
    return df_productos

def carga_producto_postgres(fila):
    db = conn2('fsalinas', False)
    sql = f'''
    INSERT INTO web_scraping.drog_cruzverde (url_producto, fecha_scraping, hora_scraping, titulo, marca_producto, nota_envio, precio_oferta, precio_original)
    SELECT *
    FROM (values{fila}) as s(url_producto, fecha_scraping, hora_scraping, titulo, marca_producto, nota_envio, precio_oferta, precio_original)
    '''
    print(str(RunDML(sql, db)[0])[:100], end='\r')
    db.close()

inicio = dt.now()
with closing(Chrome(executable_path=chromedriver, chrome_options=options)) as navegador:
    for cat_url in cat_urls:
        url = url_principal + cat_url + '?start=0&sz=5000'
        print(url)
        navegador.get(url)
        try:
            df = obtiene_productos(navegador)
            for fila in df.index:
                carga_producto_postgres(tuple(df.iloc[fila:fila+1,:].values.tolist()[0]))
        except:
            continue
print(f'Duración total: {dt.now()-inicio}')