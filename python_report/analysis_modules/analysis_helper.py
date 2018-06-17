# -*- coding: utf-8 -*-
"""
Created on Sun Jun 17 11:04:26 2018

@author: Nagasudhir
"""
import sys
# making the parent directory as the main path for imports
sys.path.append("..")
from db_connector import getConn
from log_helper import create_log_in_db
import psycopg2
from psycopg2.extras import execute_values
import xlwings as xw
import pandas as pd
import numpy as np

def set_limits_in_db(wb):
    try:
        sheetName = 'LIMITS'
        valsArr= wb.sheets[sheetName].range('A2').options(expand='table').value
        tuples_write = """
            insert into key_vals_limits (
                val_key,
                entity,
                low_val,
                high_val
            ) values %s on conflict(val_key, entity) do update set low_val = EXCLUDED.low_val, high_val = EXCLUDED.high_val
        """
        tuples = []
        conn = getConn()
        cur = conn.cursor()
        for ind in range(len(valsArr)):
            if(len(valsArr[ind]) < 3):
                continue
            entity = valsArr[ind][0]
            val_key = valsArr[ind][1]
            low_val = valsArr[ind][2]
            high_val = valsArr[ind][3]
            
            # set default limits of -10000, 10000 if the low_val, high_val are not numeric
            (isNum, val) = refine_numeric_limit_val('low', low_val)
            if(isNum==False):
                low_val = val
                create_log_in_db('non_numeric_limit_val_low', 'found a non numeric low limit val for %s, %s'%(entity, val_key))
            
            (isNum, val) = refine_numeric_limit_val('high', high_val)
            if(isNum==False):
                high_val = val
                create_log_in_db('non_numeric_limit_val_high', 'found a non numeric high limit val for %s, %s'%(entity, val_key))
            
            tuples.append(dict(val_key=val_key, entity=entity, low_val=low_val, high_val=high_val))
        
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(val_key)s,
                %(entity)s,
                %(low_val)s,
                %(high_val)s
            )""",
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()    

def refine_numeric_limit_val(limitType, val):
    defaultVal = 10000
    if(limitType == 'low'):
        defaultVal = -10000
    elif(limitType == 'high'):
        defaultVal = 10000
    if((type(val) != float and type(val) != int)):
        return (False, defaultVal)
    else:
        return (True, val)

def analyse_violations_db():
    try:
        conn = getConn()
        cur = conn.cursor()
        # create sql statement that gets all the limits info
        limits_info_sql = '''select key_vals.val_key, key_vals.entity, key_vals.val, key_vals_limits.low_val, key_vals_limits.high_val from key_vals 
        inner join key_vals_limits on key_vals.entity = key_vals_limits.entity
        and
        key_vals.val_key = key_vals_limits.val_key
        '''
        cur.execute(limits_info_sql)
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        columns = [x[0] for x in cur.description]
        limitsDF = pd.DataFrame(data=rows, columns=columns)
        # clear memory
        rows = None
        # check for non numeric key values and limit violating key values
        limitsDF['val'] = pd.to_numeric(limitsDF['val'], errors='coerce')        
        limitsDF = limitsDF[np.isnan(limitsDF['val']) | (limitsDF['val']>limitsDF['high_val']) | (limitsDF['val']<limitsDF['low_val'])]
        # add tags for the type of violation
        limitsDF['violation_tag'] = limitsDF['val'].apply(lambda x: 'non_numeric' if np.isnan(x) else 'limit_violation')
        return limitsDF
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    