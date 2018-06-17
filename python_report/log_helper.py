# -*- coding: utf-8 -*-
"""
Created on Sun Jun 10 22:57:38 2018

@author: Nagasudhir
"""
from db_connector import getConn
import psycopg2
from psycopg2.extras import execute_values
import datetime as dt

def create_log_in_db(log_priority, log_message):
    try:
        tuples = []
        if log_priority == None:
            log_priority = 'verbose'
        tuples.append(dict(log_priority=log_priority, log_message=log_message))
        
        tuples_write = """
            insert into log_messages (
                log_priority,
                log_message
            ) values %s
        """
        conn = getConn()
        cur = conn.cursor()
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(log_priority)s,
                %(log_message)s
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
        
def fetchLogsBetweenTimes(from_time, to_time):
    from_time = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) if from_time == None else from_time
    to_time = from_time + dt.timedelta(days=1) if to_time == None else to_time
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute("""SELECT log_time, log_priority, log_message from log_messages where log_time >= %s and log_time < %s""", (from_time, to_time))
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        if len(rows) > 0:
            rows = [[row[0].replace(microsecond=0)] + list(row[1:]) for row in rows]
            return rows
        else:
            return []
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
        
def deleteLogsBetweenTimes(from_time, to_time):
    from_time = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) if from_time == None else from_time
    to_time = from_time + dt.timedelta(days=1) if to_time == None else to_time
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute("""delete from log_messages where log_time >= %s and log_time < %s""", (from_time, to_time))
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()