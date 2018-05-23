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
import math
try:
    from itertools import izip as zip
except ImportError: # will be 3.x series
    pass
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


# transformation step #1
def transformStateDataAndPush():
    try:
        conn = getConn()
        cur = conn.cursor()
        # get the list of constituents
        cur.execute("""SELECT id, name, region from constituents""")
        rows = cur.fetchall()
        const_list = [x[1] for x in rows]
        # find the peak hour
        cur.execute("""SELECT val from key_vals where val_key = %s AND entity = %s""", ('peak_hrs', 'config'))
        rows = cur.fetchall()
        peak_hr = int(float(rows[0][0]))
        hourlyDemandsDict = {}
        hourlyShortageDict = {}
        maxDemMetDict = {}
        maxDemMetHrsDict = {}
        maxDemMetShortageDict = {}
        maxReqDict = {}
        maxReqHrsDict = {}
        maxReqShortageDict = {}
        peakDemDict = {}
        offPeakDemDict = {}
        peakShortageDict = {}
        offPeakShortageDict = {}
        # for ech constituent, find the max_demand_met, max_demand_met_hrs, shortage_at_max_demand_met, max_requirement, max_requirement_hrs, shortage_at_max_requirement, peak_demand_met, peak_shortage, off_peak_demand_met, off_peak_shortage
        # find 24 hr demand values for each consituent
        for cons in const_list:
            cur.execute("""SELECT hour_num, val from hour_vals where entity = %s and val_type = %s""", (cons, 'demand'))
            rows = cur.fetchall()
            # initiate the demand values
            demandVals = []
            for i in range(24):
                demandVals.append(0)
            # assign hourly values to demandVals array
            for row in rows:
                demandVals[row[0]-1] = row[1]
                hourlyDemandsDict[cons] = demandVals
            # initiate the 24 hr shortfall(ls) values
            cur.execute("""SELECT hour_num, val from hour_vals where entity = %s and val_type = %s""", (cons, 'ls'))
            rows = cur.fetchall()
            # initiate the demand values
            lsVals = []
            for i in range(24):
                lsVals.append(0)
            # assign hourly values to demandVals array
            for row in rows:
                lsVals[row[0]-1] = row[1]
                hourlyShortageDict[cons] = lsVals
            # find the max_demand_met_mw and max_demand_met_hrs, max_demand_met_shortage_mw
            maxDemMet = max(demandVals)
            maxDemMetHrs = demandVals.index(maxDemMet)
            maxDemMetShortage = lsVals[maxDemMetHrs]
            # find the max_requirement, max_requirement_hrs, shortage_at_max_requirement
            hourlyReq = [sum(x) for x in zip(demandVals, lsVals)]
            maxReq = max(hourlyReq)
            maxReqHrs = hourlyReq.index(maxReq)
            maxReqShortage = lsVals[maxReqHrs]
            # find the peak off peak demand and loadshedding
            peakDem = demandVals[peak_hr - 1]
            offPeakDem = demandVals[2]
            peakShortage = lsVals[peak_hr - 1]
            offpeakShortage = lsVals[2]
            # store all the values into the dictionary
            maxDemMetDict[cons] = maxDemMet
            maxDemMetHrsDict[cons] = maxDemMetHrs
            maxDemMetShortageDict[cons]  = maxDemMetShortage
            maxReqDict[cons]  = maxReq
            maxReqHrsDict[cons]  = maxReqHrs
            maxReqShortageDict[cons]  = maxReqShortage
            peakDemDict[cons]  = peakDem
            offPeakDemDict[cons]  = offPeakDem
            peakShortageDict[cons]  = peakShortage
            offPeakShortageDict[cons]  = offpeakShortage
            
        # find WR max_demand_met, max_demand_met_hrs, shortage_at_max_demand_met, max_requirement, max_requirement_hrs, shortage_at_max_requirement
        wrHourlyDem = [sum(x) for x in zip(*hourlyDemandsDict.values())]
        wrHourlyShortage = [sum(x) for x in zip(*hourlyShortageDict.values())]
        wrMaxDem = max(wrHourlyDem)
        wrMaxDemHrs = wrHourlyDem.index(wrMaxDem)
        wrMaxDemShortage = wrHourlyShortage[wrMaxDemHrs]    
        wrHourlyReq = [sum(x) for x in zip(wrHourlyDem, wrHourlyShortage)]
        wrMaxReq = max(wrHourlyReq)
        wrMaxReqHrs = wrHourlyReq.index(wrMaxReq)
        wrMaxReqShortage = wrHourlyShortage[wrMaxReqHrs]
        # now push the constituents values into the db
        tuples_write = """
            insert into key_vals (
                val_key,
                entity,
                val
            ) values %s on conflict(val_key, entity) do update set val = EXCLUDED.val
        """
        tuples = []
        for cons in const_list:
            # create the tuples for pushing into db
            tuples.append(dict(val_key='max_demand_met_mw', entity=cons, val=maxDemMetDict[cons]))
            tuples.append(dict(val_key='max_demand_met_hrs', entity=cons, val=maxDemMetHrsDict[cons]+1))
            tuples.append(dict(val_key='max_demand_met_shortage_mw', entity=cons, val=maxDemMetShortageDict[cons]))
            tuples.append(dict(val_key='max_requirement_mw', entity=cons, val=maxReqDict[cons]))
            tuples.append(dict(val_key='max_requirement_hrs', entity=cons, val=maxReqHrsDict[cons]+1))
            tuples.append(dict(val_key='max_requirement_shortage_mw', entity=cons, val=maxReqShortageDict[cons]))
            tuples.append(dict(val_key='peak_demand_mw', entity=cons, val=peakDemDict[cons]))
            tuples.append(dict(val_key='peak_shortfall_mw', entity=cons, val=peakShortageDict[cons]))
            tuples.append(dict(val_key='3hrs_demand_mw', entity=cons, val=offPeakDemDict[cons]))
            tuples.append(dict(val_key='3hrs_shortfall_mw', entity=cons, val=offPeakShortageDict[cons]))
        # add wr values to tuples
        tuples.append(dict(val_key='max_demand_met_mw', entity='wr', val=wrMaxDem))
        tuples.append(dict(val_key='max_demand_met_hrs', entity='wr', val=wrMaxDemHrs+1))
        tuples.append(dict(val_key='max_demand_met_shortage_mw', entity='wr', val=wrMaxDemShortage))
        tuples.append(dict(val_key='max_requirement_mw', entity='wr', val=wrMaxReq))
        tuples.append(dict(val_key='max_requirement_hrs', entity='wr', val=wrMaxReqHrs+1))
        tuples.append(dict(val_key='max_requirement_shortage_mw', entity='wr', val=wrMaxReqShortage))
        
        # push tuples to db
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(val_key)s,
                %(entity)s,
                %(val)s
            )""",
            page_size = 500
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print e
    finally:
        conn.close()

# transformation step #2
def transformVoltDataAndPush():
    try:
        conn = getConn()
        cur = conn.cursor()
        # get the list of volatge stations for voltage level info
        cur.execute("""SELECT scada_id, volt from volt_level_info""")
        rows = cur.fetchall()
        voltLevelInfoDict = {}
        for row in rows:
            voltLevelInfoDict[row[0]] = row[1]
        # get all the scada volt headings
        cur.execute("""SELECT distinct entity from minute_vals where val_type = 'volt'""")
        rows = cur.fetchall()
        voltStationsList = []
        for row in rows:
            voltStationsList.append(row[0])
        voltInfoDict = {}
        # for each volt station find max, max_time, min, min_time, band_voltage_percentages
        tuples = []
        for voltNode in voltStationsList:
            # todo handle if station not present in voltLevelInfoDict
            nodeDict = dict(volt=float(voltLevelInfoDict[voltNode]), max=0, min=0, min_time='00:00', max_time='00:00', band1_perc=0, band2_perc=0, band3_perc=0, band4_perc=0)
            voltInfoDict[voltNode] = nodeDict
            # get the 1440 min values
            cur.execute("""SELECT val from minute_vals where val_type='volt' and entity=%s order by min_num asc""", (voltNode,))
            rows = cur.fetchall()
            voltageVals = []
            if(len(rows) != 1440):
                continue
            for row in rows:
                voltageVals.append(row[0])
            nodeDict['max'] = max(voltageVals)
            nodeDict['min'] = min(voltageVals)
            nodeDict['max_time'] = convert_min_to_time_str(voltageVals.index(nodeDict['max']))
            nodeDict['min_time'] = convert_min_to_time_str(voltageVals.index(nodeDict['min']))
            min1 = 380 if nodeDict['volt'] == 400 else 720
            min2 = 390 if nodeDict['volt'] == 400 else 750
            max1 = 420 if nodeDict['volt'] == 400 else 780
            max2 = 430 if nodeDict['volt'] == 400 else 800
            nodeDict['band1_perc'] = sum([1 for x in voltageVals if x<min1])/14.4
            nodeDict['band2_perc'] = sum([1 for x in voltageVals if x<min2])/14.4
            nodeDict['band3_perc'] = sum([1 for x in voltageVals if x>max1])/14.4
            nodeDict['band4_perc'] = sum([1 for x in voltageVals if x>max2])/14.4
            voltInfoDict[voltNode] = nodeDict
            tuples.append(dict(val_key='max_volt', entity=voltNode, val=nodeDict['max']))
            tuples.append(dict(val_key='min_volt', entity=voltNode, val=nodeDict['min']))
            tuples.append(dict(val_key='max_volt_time', entity=voltNode, val=nodeDict['max_time']))
            tuples.append(dict(val_key='min_volt_time', entity=voltNode, val=nodeDict['min_time']))
            tuples.append(dict(val_key='band1_volt_perc', entity=voltNode, val=nodeDict['band1_perc']))
            tuples.append(dict(val_key='band2_volt_perc', entity=voltNode, val=nodeDict['band2_perc']))
            tuples.append(dict(val_key='band3_volt_perc', entity=voltNode, val=nodeDict['band3_perc']))
            tuples.append(dict(val_key='band4_volt_perc', entity=voltNode, val=nodeDict['band4_perc']))
        tuples_write = """
            insert into key_vals (
                val_key,
                entity,
                val
            ) values %s on conflict(val_key, entity) do update set val = EXCLUDED.val
        """
        # push tuples to db
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(val_key)s,
                %(entity)s,
                %(val)s
            )""",
            page_size = 500
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print e
    finally:
        conn.close()

def convert_min_to_time_str(minReq):
    hrs = math.floor(minReq/60)
    mins = minReq - hrs*60
    return '{0}:{1}'.format(str(int(hrs)).zfill(2), str(int(mins)).zfill(2))
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