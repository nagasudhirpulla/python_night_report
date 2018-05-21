# -*- coding: utf-8 -*-
"""
Created on Thu May 17 22:26:46 2018

@author: Nagasudhir
"""
# Basic module Usage - http://www.psycopg.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries
# Restart counter of table - https://stackoverflow.com/questions/5342440/reset-auto-increment-counter-in-postgres
# Passing array of parameters to IN statement - https://stackoverflow.com/questions/28117576/python-psycopg2-where-in-statement
# Insert multiple rows https://stackoverflow.com/questions/46373804/using-unnest-with-psycopg2
# Update values on conflict during insert https://dba.stackexchange.com/questions/167591/postgresql-psycopg2-upsert-syntax-to-update-columns

import psycopg2
import ids_helper
from psycopg2.extras import execute_values

def getConn():
    conn = None
    try:
        conn = psycopg2.connect("dbname='night_report_db' user='postgres' host='localhost' password='123'")
        #print 'connection done...'        
    except:
        print "I am unable to connect to the database"
    return conn

def push_config_to_db(wb):
    config_df = ids_helper.get_config_df(wb)
    conn = getConn()
    cur = conn.cursor()
    tuples = []
    for indexVal in config_df.index.values:
        tuples.append(dict(val_key=indexVal, val=str(config_df.loc[indexVal][0]), entity='config'))
    tuples_write = """
        insert into key_vals (
            val_key,
            entity,
            val
        ) values %s on conflict(val_key, entity) do update set val = EXCLUDED.val
    """
    conn = getConn()
    cur = conn.cursor()
    execute_values (
        cur,
        tuples_write,
        tuples,
        template = """(
            %(val_key)s,
            %(entity)s,
            %(val)s
        )""",
        page_size = 1000
    )
    conn.commit()
    cur.close()
    conn.close()

def getDBConfigVal(config_params):
    conn = getConn()
    cur = conn.cursor()
    cur.execute("""SELECT val_key, val from key_vals where val_key IN %s and entity=%s""", (tuple(config_params), 'config'))
    rows = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    if len(rows) > 0:
        config_dict = dict((x, y) for x, y in rows)
        return config_dict
    else:
        return None

def push_sch_to_db(wb):
    tuples_write = """
        insert into blk_vals (
            sch_type,
            entity,
            blk,
            val
        ) values %s on conflict(sch_type, entity, blk) do update set val = EXCLUDED.val
    """
    # firstBlkRow = getDBConfigVal(['sch_first_blk_row'])['sch_first_blk_row']
    schedulesSheet = 'SCH'
    entitiesList= wb.sheets[schedulesSheet].range('B2').options(expand='right').value
    schTypesList= wb.sheets[schedulesSheet].range('B3').options(expand='right').value
    valsArr= wb.sheets[schedulesSheet].range('B4').options(expand='table').value
    excludedSchTypes = ['Time Block', 'Time Desc']
    conn = getConn()
    # cur = conn.cursor()
    tuples = []
    for entityIndex in range(len(entitiesList)):
        entity = entitiesList[entityIndex]
        schType = schTypesList[entityIndex]
        if schType in excludedSchTypes:
            continue
        # Dump the 96 block values into the DB
        cur = conn.cursor()
        tuples = []
        for blk in range(1,97):
            rowInd = blk - 1
            val = valsArr[rowInd][entityIndex]
            tuples.append(dict(sch_type=schType, entity=entity, blk=blk, val = val))        
        
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(sch_type)s,
                %(entity)s,
                %(blk)s,
                %(val)s
            )""",
            page_size = 1450
        )
        conn.commit()
    cur.close()
    conn.close()
    
def push_scada_to_db(wb):
    tuples_write = """
        insert into minute_vals (
            val_type,
            entity,
            min_num,
            val
        ) values %s on conflict(val_type, entity, min_num) do update set val = EXCLUDED.val
    """
    scadaSheet = 'SCADA'
    entitiesList= wb.sheets[scadaSheet].range('B1').options(expand='right').value
    valTypesList= wb.sheets[scadaSheet].range('B2').options(expand='right').value
    valsArr= wb.sheets[scadaSheet].range('B3').options(expand='table').value
    excludedEntTypes = ['Timestamp']
    conn = getConn()
    # cur = conn.cursor()
    tuples = []
    for entityIndex in range(len(entitiesList)):
        entity = entitiesList[entityIndex]
        valType = valTypesList[entityIndex]
        if entity in excludedEntTypes:
            continue
        # Dump the 96 block values into the DB
        cur = conn.cursor()
        tuples = []
        for min_num in range(1,1441):
            rowInd = min_num - 1
            val = valsArr[rowInd][entityIndex]
            tuples.append(dict(val_type=valType, entity=entity, min_num=min_num, val = val))        
        
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(val_type)s,
                %(entity)s,
                %(min_num)s,
                %(val)s
            )""",
            page_size = 1450
        )
        conn.commit()
    cur.close()
    conn.close()
    
def push_hourly_to_db(wb):
    tuples_write = """
        insert into hour_vals (
            val_type,
            entity,
            hour_num,
            val
        ) values %s on conflict(val_type, entity, hour_num) do update set val = EXCLUDED.val
    """
    sheetName = 'HOURLY'
    entitiesList= wb.sheets[sheetName].range('A1').options(expand='right').value
    valTypesList= wb.sheets[sheetName].range('A2').options(expand='right').value
    valsArr= wb.sheets[sheetName].range('A3').options(expand='table').value
    conn = getConn()
    # cur = conn.cursor()
    tuples = []
    for entityIndex in range(len(entitiesList)):
        entity = entitiesList[entityIndex]
        valType = valTypesList[entityIndex]
        # Dump the 24 HOUR values into the DB
        cur = conn.cursor()
        tuples = []
        for hour_num in range(1,25):
            rowInd = hour_num - 1
            val = valsArr[rowInd][entityIndex]
            tuples.append(dict(val_type=valType, entity=entity, hour_num=hour_num, val = val))        
        
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(val_type)s,
                %(entity)s,
                %(hour_num)s,
                %(val)s
            )""",
            page_size = 50
        )
        conn.commit()
    cur.close()
    conn.close()
    
def push_key_vals_to_db(wb):
    sheetName = 'KEYVALUES'
    valsArr= wb.sheets[sheetName].range('A1').options(expand='table').value
    tuples_write = """
        insert into key_vals (
            val_key,
            entity,
            val
        ) values %s on conflict(val_key, entity) do update set val = EXCLUDED.val
    """
    tuples = []
    conn = getConn()
    cur = conn.cursor()
    for ind in range(len(valsArr)):
        if(len(valsArr[ind]) < 3):
            continue
        entity = valsArr[ind][0]
        val_key = valsArr[ind][1]
        val = valsArr[ind][2]
        tuples.append(dict(val_key=val_key, entity=entity, val=val))
    execute_values (
        cur,
        tuples_write,
        tuples,
        template = """(
            %(val_key)s,
            %(entity)s,
            %(val)s
        )""",
        page_size = 1000
    )
    conn.commit()
    cur.close()
    conn.close()

def get_db_scada_val(wb, metricsList):
    # stub
    
'''
# Insert a row
conn = getConn()
cur = conn.cursor()
cur.execute('INSERT INTO key_vals (val_key, entity, val) VALUES (%s, %s, %s)', ('solar_mu', 'dnh', '3241'))
conn.commit()
cur.close()
conn.close()
'''

'''
# Insert multiple rows
tuples = [
    dict (
        val_key = 'wind',
        entity = 'dnh',
        val = '9654'
    ),
    dict (
        val_key = 'hydro',
        entity = 'dd',
        val = '6248'
    )
]

tuples_write = """
    insert into key_vals (
        val_key,
        entity,
        val
    ) values %s on conflict(val_key, entity) do update set val = EXCLUDED.val
"""

conn = getConn()
cur = conn.cursor()
execute_values (
    cur,
    tuples_write,
    tuples,
    template = """(
        %(val_key)s,
        %(entity)s,
        %(val)s
    )""",
    page_size = 1000
)
conn.commit()
cur.close()
conn.close()
'''

'''
# Read all rows
conn = getConn()
cur = conn.cursor()
cur.execute("""SELECT id, val_key, entity, val from key_vals""")
rows = cur.fetchall()
conn.commit()
cur.close()
conn.close()
'''