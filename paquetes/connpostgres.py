# -*- coding: utf-8 -*-
"""
Created on Mon Jun 14 12:15:13 2021

@author: fsalinas
"""

def conn2(usuario, show_msg = False):
    '''
    Retorna cadena de conexión a PostgreSQL
    Opciones disponibles:
        user:
            postgres
            fsalinas
            cgomez
    '''
    pwd = {'postgres':'aadd4455', 'fsalinas':'fsalinas1547', 'cgomez':'cgomez4526'}[usuario]
    host, port, db = 'localhost', '5432', 'boticarios'
    
    import psycopg2
    conn = psycopg2.connect(user = usuario, password = pwd, host = host, port = port, database = db)
    if show_msg: print(f'Conectado a {db} con el usuario {usuario}')
    return(conn)