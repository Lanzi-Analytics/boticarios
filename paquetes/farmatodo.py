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
url_principal = 'https://www.farmatodo.com.co'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
    # navegador = Chrome(executable_path = chromedriver, options=options)
    navegador.get(url_principal)
    soup = BeautifulSoup(navegador.page_source, 'html.parser')

    cat_urls = [x.get_attribute_list('href')[0].replace(url_principal, '') for x in soup.find_all('a') if x.get_attribute_list('href')[0]!=None and '/categorias' in x.get_attribute_list('href')[0]]
    cat_urls = list(dict.fromkeys(cat_urls))

    # Guarda la lista de URLs en un archivo csv
    with open('./web_scraping/data/cat_urls_farmatodo.csv', 'w+') as f:
        f.write('\n'.join(cat_urls))

    print(f'Número de urls de categorias: {len(cat_urls)}')
    cat_urls[:10]

# Define rutas a urls y archivos
chromedriver = './web_scraping/chromedriver/chromedriver.exe'
url_principal = 'https://www.farmatodo.com.co'

# Define si el navegador estará visible durante el proceso
hide_browser = False

# Aplica opciones al navegador para evitar cargar recursos innecesarios
options = Options()
options.add_argument('--ignore-certificate-errors')
if hide_browser: options.add_argument('--headless')
options.add_argument('--disable-dev-shm-usage')
options.add_experimental_option('prefs',{'profile.managed_default_content_setings.images':2})

# Leer el archivo con las url de categorias
with open('./web_scraping/data/cat_urls_farmatodo.csv', 'r') as f:
    cat_urls = f.read()
cat_urls = cat_urls.split('\n')

def obtiene_data_producto(soup):
    url = '/producto/' + str(soup).split('data-cuf="')[1].split('"')[0]
    try:
        titulo = soup.find_all('p', {'class': 'text-title'})[0].text
    except:
        titulo = 'No conseguido'
    
    try:
        presentacion = soup.find_all('p', {'class': 'text-description'})[0].text.strip()
    except:
        presentacion = 'No conseguido'
    
    try:
        precio_final = float(''.join([x for x in soup.find_all('span', {'class': 'text-price'})[0].text.strip() if x.isdigit()]))
    except:
        precio_final = 0
    
    try:
        precio_tachado = float(''.join([x for x in soup.find_all('span', {'class': 'text-offer-price'})[0].text.strip() if x.isdigit()]))
    except:
        precio_tachado = 0
    
    try:
        precio_por = soup.find_all('p', {'class': 'text-price-unit'})[0].text.strip()
    except:
        precio_por = 0
    
    try:
        calificacion = float(soup.find_all('div', {'class': 'bv_averageRating_component_container'})[0].text)
        numero_calificaciones = int(soup.find_all('div', {'class': 'bv_numReviews_component_container'})[0].text.replace('(','').replace(')',''))
    except:
        calificacion = 0
        numero_calificaciones = 0
    
    try:
        tiempo_entrega = soup.find_all('div', {'class': 'calendar hide-r'})[0].text
    except:
        tiempo_entrega = 'N/A'
    
    df = pd.DataFrame.from_dict(
        data = {
            'url': url,
            'fecha_scraping': dt.now().strftime('%Y-%m-%d'),
            'hora_scraping': dt.now().strftime('%H:%M:%S'),
            'titulo': titulo,
            'presentacion': presentacion,
            'precio_por': precio_por,
            'calificacion': calificacion,
            'numero_calificaciones': numero_calificaciones,
            'tiempo_entrega': tiempo_entrega,
            'precio_tachado': precio_tachado,
            'precio_final': precio_final
        },
        orient = 'index'
    )
    return df

def carga_producto_postgres(fila):
    db = conn2('fsalinas', False)
    sql = f'''
    INSERT INTO web_scraping.drog_farmatodo (url_producto, fecha_scraping, hora_scraping, titulo, presentacion, precio_por, calificacion, numero_calificaciones, tiempo_entrega, precio_tachado, precio_final)
    SELECT *
    FROM (values{fila}) as s(url_producto, fecha_scraping, hora_scraping, titulo, presentacion, precio_por, calificacion, numero_calificaciones, tiempo_entrega, precio_tachado, precio_final)
    '''
    print(str(RunDML(sql, db)[0])[:100], end='\r')
    db.close()

def recorre_url_cat(navegador, url):
    navegador.get(url)
    time.sleep(5)
    try:
        boton_cargar_mas = navegador.find_element_by_id('group-view-load-more')
        while True:
            try:
                boton_cargar_mas.send_keys(Keys.PAGE_DOWN)
                boton_cargar_mas.click()
                time.sleep(1)
            except:
                break
    except:
        'continue'
    
    time.sleep(5)
    l_soup = BeautifulSoup(navegador.page_source, 'html.parser').find_all('div', {'class': 'card-ftd'})

    df_concat = pd.concat([obtiene_data_producto(soup) for soup in l_soup], axis=1, ignore_index=True).T.drop_duplicates(subset='url').reset_index().iloc[:,1:]
    return df_concat

inicio = dt.now()
print(f'{inicio.strftime("%H:%M:%S")}: Inicio Proceso')
with closing(Chrome(executable_path = chromedriver, options=options)) as navegador:
	for url in cat_urls:
		try:
			tdf = recorre_url_cat(navegador, url_principal + url)
			for x in tdf.index:
				try:
					carga_producto_postgres(tuple(tdf.iloc[x:x+1,:].values.tolist()[0]))
				except:
					print('Producto no cargado...', end='\r')
		except:
			print('Producto no cargado...', end='\r')

print(f'{dt.now().strftime("%H:%M:%S")}: Fin Proceso')
print(f'Duración: {dt.now()-inicio}')