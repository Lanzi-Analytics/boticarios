# -*- coding: utf-8 -*-
"""
Created on Wed Jul 29 14:57:08 2020

@author: fsalinas
"""

import pandas as pd
import psycopg2
from datetime import datetime as dt

def RunDML(SQL, cn):
    '''
    DML: Data Manipulation Language -> INSERT, UPDATE, DELETE, DROP, TRUNCATE...
    Parameters
    ----------
    SQL : query string
        must be a Data Manipulation Language (INSERT, UPDATE, DELETE, DROP...).
    cn : connpostgres object
        Connection to postgreSQL.
    Returns
    -------
    msg : String message with the result of the query.
    telapsed : time elapsed from the start of the execution of the querie to the end.
    '''
    start = dt.now()
    try:
        cr = cn.cursor()
        cr.execute(SQL)
        nrows = 0 if cr.rowcount is None else cr.rowcount
        cn.commit()
        cr.close()
        msg = 'OK, {n:,.0f} rows affected.'.format(n = nrows)
    except psycopg2.DatabaseError as error:
        cr.close()
        msg = 'KO: {e}\n{s}Start Query{s}\n{q}\n{s}End Query{s}'.format(e = error, s = '|'*25, q = SQL)
    telapsed = dt.now() - start
    return(msg, telapsed)

def RunDDL(SQL, cn):
    '''
    DDL: Data Definition Language -> SELECT.
    Parameters
    ----------
    SQL : query string
        must be a Data Definition Language (SELECT.).
    cn : connpostgres object
        Connection to postgreSQL.
    Returns
    -------
    df : Pandas Dataframe object with the result of the query applied.
    telapsed : time elapsed from the start of the execution of the querie to the end.
    '''
    start = dt.now()
    try:
        df = pd.read_sql(SQL, cn)
        telapsed = dt.now() - start
        return(df, telapsed)
    except psycopg2.DatabaseError as error:
        msg = 'KO: {e}\n{s}Start Query{s}\n{q}\n{s}End Query{s}'.format(e = error, s = '|'*25, q = SQL)
        return(msg)