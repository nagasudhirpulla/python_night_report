# -*- coding: utf-8 -*-
"""
Created on Mon Jun 11 00:04:28 2018

@author: Nagasudhir
"""
import psycopg2

def getConn():
    conn = None
    try:
        conn = psycopg2.connect("dbname='night_report_db' user='postgres' host='localhost' password='123'")
        #print('connection done...')
    except:
        print("I am unable to connect to the database")
    return conn