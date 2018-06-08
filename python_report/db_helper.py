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
        #print('connection done...')
    except:
        print("I am unable to connect to the database")
    return conn

def push_constituents_db():
    try:
        tuples = []
        constituents = ['cseb', 'dd', 'dnh', 'esil', 'geb', 'goa', 'mp', 'mseb']
        for cons in constituents:
            tuples.append(dict(name=cons, region='wr'))
        tuples_write = """
            insert into constituents (
                name,
                region
            ) values %s on conflict(name) DO NOTHING
        """
        conn = getConn()
        cur = conn.cursor()
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(name)s,
                %(region)s
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
    
def push_config_to_db(wb):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

def getDBConfigVal(config_params):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

def push_sch_to_db(wb):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    
def push_scada_to_db(wb):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    
def push_hourly_to_db(wb):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    
def push_key_vals_to_db(wb):
    try:
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass
 
    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False

def push_ire_manual_to_db(wb):
    try:
        sheetName = 'IRE_MANUAL_DB'
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
        for ind in range(1, len(valsArr)):
            if(len(valsArr[ind]) < 3):
                continue
            entities = valsArr[ind][0].split('|')
            importMUVal = ''
            exportMUVal = ''
            if(is_number(valsArr[ind][3])):
                importMUVal = float(valsArr[ind][3]) / len(entities)
            if(is_number(valsArr[ind][4])):
                exportMUVal = float(valsArr[ind][4]) / len(entities)        
            for entity in entities:            
                tuples.append(dict(val_key='import_mu', entity=entity, val=importMUVal))
                tuples.append(dict(val_key='export_mu', entity=entity, val=exportMUVal))
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
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

def transformRawData():
    transformStateDataAndPush()
    transformVoltDataAndPush()
    transformGenRawStateGenDataAndPush()
    transformStateSchDataAndPush()
    transformIRSchDataAndPush()
    transformIRLinesScadaDataAndPush()
    transformGenDCSchDataAndPush()

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
        shortfallMUDict = {}
        # for ech constituent, find the max_demand_met, max_demand_met_hrs, shortage_at_max_demand_met, max_requirement, max_requirement_hrs, demand_met_at_max_requirement, shortage_at_max_requirement, peak_demand_met, peak_shortage, off_peak_demand_met, off_peak_shortage
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
            shortage_mu = sum(lsVals)/1000
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
            shortfallMUDict[cons] = shortage_mu
            
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
            tuples.append(dict(val_key='shortfall_mu', entity=cons, val=shortfallMUDict[cons]))
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
        print(e)
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
            nodeDict = dict(volt=float(voltLevelInfoDict[voltNode]), max=0, min=0, min_time='00:00', avg=0, max_time='00:00', band1_perc=0, band2_perc=0, band3_perc=0, band4_perc=0)
            voltInfoDict[voltNode] = nodeDict
            # get the 1440 min values
            cur.execute("""SELECT val from minute_vals where val_type='volt' and entity=%s order by min_num asc""", (voltNode,))
            rows = cur.fetchall()
            voltageVals = []
            if(len(rows) == 1440):
                for row in rows:
                    voltageVals.append(row[0])
                nodeDict['max'] = max(voltageVals)
                nodeDict['min'] = min(voltageVals)
                nodeDict['avg'] = sum(voltageVals)/1440
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
            tuples.append(dict(val_key='avg_volt', entity=voltNode, val=nodeDict['avg']))
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
        print(e)
    finally:
        conn.close()

# transformation step #3
def transformGenRawStateGenDataAndPush():
    try:
        conn = getConn()
        cur = conn.cursor()
        # find the peak hour
        cur.execute("""SELECT val from key_vals where val_key = %s AND entity = %s""", ('peak_hrs', 'config'))
        rows = cur.fetchall()
        peak_blk_Ind = int(float(rows[0][0]))*4
        # get the list of generators for state gen info
        cur.execute("""SELECT DISTINCT entity from minute_vals where val_type in ('state_gen', 'gen_raw', 'state_raw') order by entity asc""")
        rows = cur.fetchall()
        stateGenList = []
        for row in rows:
            stateGenList.append(row[0])
        stateGenDict = {}
        tuples = []
        # for state_gen find peak_mw, off_peak_mw, max_mw, max_mw_hrs, avg_mw
        for stateGenCounter, stateGen in enumerate(stateGenList):
            genDict = dict(peak_blk_mw=0, off_peak_blk_mw=0, max_blk_mw=0, max_blk_mw_time=0, avg_mw=0)
            stateGenDict[stateGen] = genDict
            # get the minute values of the generator            
            cur.execute("""SELECT val from minute_vals where val_type IN ('gen_raw', 'state_gen', 'state_raw') and entity=%s order by min_num asc""", (stateGen, ))
            rows = cur.fetchall()
            if(len(rows) == 1440):
                minVals = []
                for row in rows:
                    minVals.append(row[0])
                # convert minVals into blkVals
                blkVals = []
                for blkInd in range(96):
                    blkVals.append(sum(minVals[(blkInd*15):((blkInd+1)*15)])/15)
                genDict['peak_blk_mw'] = blkVals[peak_blk_Ind]
                genDict['off_peak_blk_mw'] = blkVals[2]
                genDict['max_blk_mw'] = max(blkVals)
                genDict['max_blk_mw_time'] = '-'.join(convert_blk_to_time_strs(blkVals.index(genDict['max_blk_mw'])+1))
                genDict['avg_mw'] = sum(blkVals)/96
            for keyStr in genDict.keys():
                tuples.append(dict(val_key=keyStr, entity=stateGen, val=genDict[keyStr]))
            stateGenDict[stateGen] = genDict
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

# transformation step #4
def transformStateSchDataAndPush():
    # for each state find peak, off_peak stoa, pxil, iexl sch_mw,mus
    # for each state find max, min isgs+lta+mtoa, stoa, iexl, pxil sch_mw
    try:
        conn = getConn()
        cur = conn.cursor()
        # find the peak hour
        cur.execute("""SELECT val from key_vals where val_key = %s AND entity = %s""", ('peak_hrs', 'config'))
        rows = cur.fetchall()
        peak_blk_Ind = int(float(rows[0][0]))*4
        # get the list of generators for state gen info
        cur.execute("""SELECT distinct name from constituents""")
        rows = cur.fetchall()
        consList = []
        for row in rows:
            consList.append(row[0])
        consSchDict = {}
        tuples = []
        for cons in consList:
            schDict = dict(bilateral_3hrs_mw=0, iex_3hrs_mw=0, pxil_3hrs_mw=0,
                           bilateral_peak_mw=0, iex_peak_mw=0, pxil_peak_mw=0, 
                           isgs_lt_mt_sch_mu=0, stoa_mu=0, exchange_mu=0, total_sch_mu=0,
                           max_isgs_lt_mt_mw=0, min_isgs_lt_mt_mw=0, max_stoa_mw=0, min_stoa_mw=0,
                           max_iex_mw=0, min_iex_mw=0, max_pxil_mw=0, min_pxil_mw=0)
            consSchDict[cons] = schDict
            bilateralSchVals = []
            # get stoa vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('STOA', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                bilateralSchVals.append(row[0])
            schDict['bilateral_3hrs_mw'] = bilateralSchVals[2]
            schDict['bilateral_peak_mw'] = bilateralSchVals[peak_blk_Ind]
            schDict['stoa_mu'] = sum(bilateralSchVals)/4000
            schDict['max_stoa_mw'] = max(bilateralSchVals)
            schDict['min_stoa_mw'] = min(bilateralSchVals)
            iexVals = []
            # get iex vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('IEX', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                iexVals.append(row[0])
            schDict['iex_3hrs_mw'] = iexVals[2]
            schDict['iex_peak_mw'] = iexVals[peak_blk_Ind]
            schDict['exchange_mu'] = schDict['exchange_mu'] + sum(iexVals)/4000
            schDict['max_iex_mw'] = max(iexVals)
            schDict['min_iex_mw'] = min(iexVals)
            pxVals = []
            # get px vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('PXI', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                pxVals.append(row[0])
            schDict['pxil_3hrs_mw'] = pxVals[2]
            schDict['pxil_peak_mw'] = pxVals[peak_blk_Ind]
            schDict['exchange_mu'] = schDict['exchange_mu'] + sum(pxVals)/4000
            schDict['max_pxil_mw'] = max(pxVals)
            schDict['min_pxil_mw'] = min(pxVals)
            totSchVals = []
            # get px vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('Net Total', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                totSchVals.append(row[0])
            schDict['total_sch_mu'] = sum(totSchVals)/4000
            # get isgs_lt_mt vals
            isgsLtMtVals = []
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('ISGS', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                isgsLtMtVals.append(row[0])
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('LTA', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] + row[0]
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('MTOA', cons))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] + row[0]
            schDict['isgs_lt_mt_sch_mu'] = sum(isgsLtMtVals)/4000
            schDict['min_isgs_lt_mt_mw'] = min(isgsLtMtVals)
            schDict['max_isgs_lt_mt_mw'] = max(isgsLtMtVals)
            for keyStr in schDict.keys():
                tuples.append(dict(val_key=keyStr, entity=cons, val=schDict[keyStr]))
            consSchDict[cons] = schDict
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

# transformation step #5
def transformIRSchDataAndPush():
    # for each interregional path find, isgs, lta, mtoa, stoa, pxil, iexl, total ir schedules
    try:
        conn = getConn()
        cur = conn.cursor()
        irPaths = [dict(name='wr-nr', exp='west_north', imp='north_west'), 
                   dict(name='wr-er', exp='west_east', imp='east_west'), 
                   dict(name='wr-sr', exp='west_south', imp='south_west')]
        irPathSchDict = {}
        tuples = []
        for irPath in irPaths:
            pathSchDict = dict(isgs_lta_mtoa_mu=0, stoa_mu=0, px_mu=0, net_sch_mu=0)
            irPathSchDict[irPath['name']] = pathSchDict
            # get isgs_import sch
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('ISGS', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            isgsLtMtVals = []
            for row in rows:
                isgsLtMtVals.append(row[0])
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('ISGS', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] - rows[i][0]
            # getting lta values
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('LTA', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] + rows[i][0]
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('LTA', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] - rows[i][0]
            # getting mtoa values
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('MTOA', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] + rows[i][0]
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('MTOA', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                isgsLtMtVals[i] = isgsLtMtVals[i] - rows[i][0]
            pathSchDict['isgs_lta_mtoa_mu'] = sum(isgsLtMtVals)/4000
            # getting stoa values
            stoaVals = []
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('STOA', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                stoaVals.append(row[0])
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('STOA', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                stoaVals[i] = stoaVals[i] - rows[i][0]
            pathSchDict['stoa_mu'] = sum(stoaVals)/4000
            # getting pxi values
            exchangeVals = []
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('PXI', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                exchangeVals.append(row[0])
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('PXI', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                exchangeVals[i] = exchangeVals[i] - rows[i][0]
            # getting iex values
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('IEX', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                exchangeVals[i] = exchangeVals[i] + rows[i][0]
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('IEX', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                exchangeVals[i] = exchangeVals[i] - rows[i][0]           
            pathSchDict['px_mu'] = sum(exchangeVals)/4000
            # getting net_sch values
            netSchVals = []
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('Net Total', irPath['imp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                netSchVals.append(row[0])
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", ('Net Total', irPath['exp']))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for i in range(96):
                netSchVals[i] = netSchVals[i] - rows[i][0]
            pathSchDict['net_sch_mu'] = sum(netSchVals)/4000
            # push the path summary values to the tuples
            for keyStr in pathSchDict.keys():
                tuples.append(dict(val_key=keyStr, entity=irPath['name'], val=pathSchDict[keyStr]))            
            irPathSchDict[irPath['name']] = pathSchDict
        # push all the tuples to db
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

# get the ire lines export_mu, import_mu dict from db
def fetchDBIRElinesExportImportMUDict():
    try:
        conn = getConn()
        cur = conn.cursor()
        lineExpDict = {}
        lineImpDict = {}
        cur.execute("""SELECT entity, val_key, val from key_vals where val_key IN ('export_mu', 'import_mu')""")
        rows = cur.fetchall()
        for row in rows:
            if row[1] == 'export_mu':
                lineExpDict[row[0]] = row[2]
            elif row[1] == 'import_mu':
                lineImpDict[row[0]] = row[2]
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
        lineExpDict = {}
        lineImpDict = {}
    finally:
        conn.close()
        return (lineImpDict, lineExpDict)

# not required
def transformIRScadaDataAndPush():
    # for interregional_lines find max_export, max_export_hrs, max_import, max_import_hrs, export_mu, import_mu
    try:
        # get export import mu values from ire manual data of db
        IRElineImpDict, IRElineExpDict = fetchDBIRElinesExportImportMUDict()
        conn = getConn()
        cur = conn.cursor()
        tuples = []
        # get the list of interregional lines for ir flow info
        lineNamesList = []
        cur.execute("""SELECT DISTINCT entity from minute_vals where val_type = 'inter_regional'""")
        rows = cur.fetchall()
        for row in rows:
            lineNamesList.append(row[0])
        lineFlowDict = {}
        for lineName in lineNamesList:
            lineSummaryDict = dict(max_export=0, max_export_hrs='00:00', export_mu=0, max_import=0, max_import_hrs='00:00', import_mu=0)
            lineFlowDict[lineName] = lineSummaryDict
            # get the minute values of the line flow            
            cur.execute("""SELECT val from minute_vals where val_type = 'inter_regional' and entity=%s order by min_num asc""", (lineName,))
            rows = cur.fetchall()
            if(len(rows) != 1440):
                continue
            minVals = []
            for row in rows:
                minVals.append(-1*row[0])
            # find the max_export (+ve values) and make save the result as negative
            exportVals = [(lambda x: (0 if x>0 else x))(x) for x in minVals]
            max_export = min(exportVals)
            export_mu = sum(exportVals)/60000
            max_export_hrs = convert_min_to_time_str(exportVals.index(max_export))
            importVals = [(lambda x: (0 if x<0 else x))(x) for x in minVals]
            max_import = max(importVals)
            import_mu = sum(importVals)/60000
            max_import_hrs = convert_min_to_time_str(importVals.index(max_import))
            lineSummaryDict['max_export'] = max_export
            # assign computed scada value only if ire manual data is not present
            if hasattr(IRElineImpDict, lineName) and IRElineImpDict[lineName] != '':
                lineSummaryDict['import_mu'] = IRElineImpDict[lineName]
            else:
                lineSummaryDict['import_mu'] = import_mu
            
            if hasattr(IRElineExpDict, lineName) and IRElineExpDict[lineName] != '':
                lineSummaryDict['export_mu'] = IRElineExpDict[lineName]
            else:
                lineSummaryDict['export_mu'] = export_mu
                
            lineSummaryDict['max_export_hrs'] = max_export_hrs
            lineSummaryDict['max_import'] = max_import
            lineSummaryDict['max_import_hrs'] = max_import_hrs
            # push the path summary values to the tuples
            for keyStr in lineSummaryDict.keys():
                tuples.append(dict(val_key=keyStr, entity=lineName, val=lineSummaryDict[keyStr]))
            lineFlowDict[lineName] = lineSummaryDict
        # push all the tuples to db
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

# transformation step #6
def transformIRLinesScadaDataAndPush():
    # stub for interregional_lines find max_export, max_export_hrs, max_import, max_import_hrs, export_mu, import_mu, peak_flow_mw, off_peak_flow_mw
    try:
        # get export import mu values from ire manual data of db
        IRElineImpDict, IRElineExpDict = fetchDBIRElinesExportImportMUDict()
        conn = getConn()
        cur = conn.cursor()
        # find the peak hour
        cur.execute("""SELECT val from key_vals where val_key = %s AND entity = %s""", ('peak_hrs', 'config'))
        rows = cur.fetchall()
        peak_hr = int(float(rows[0][0]))
        peak_min = peak_hr*60
        off_peak_min = 180
        tuples = []
        # get the list of interregional lines for ir flow info
        lineNamesList = []
        lineDetailsDict = {}
        cur.execute("""SELECT line_name, ckt_names, region from line_details""")
        rows = cur.fetchall()
        for row in rows:
            lineNamesList.append(row[0])
            lineDetailsDict[row[0]] = dict(ckt_names=row[1], region=row[2])
        lineFlowDict = {}
        for lineCounter, lineName in enumerate(lineNamesList):
            lineSummaryDict = dict(peak_flow_mw=0, off_peak_flow_mw=0, max_export=0, max_export_hrs='00:00', export_mu=0, max_import=0, max_import_hrs='00:00', import_mu=0)
            lineFlowDict[lineName] = lineSummaryDict
            minVals = [0 for x in range(1440)]
            cktNames = lineDetailsDict[lineName]['ckt_names'].split('|')
            import_mu = 0.0
            export_mu = 0.0
            for cktName in cktNames:
                cktMinVals = [0 for x in range(1440)]
                # get the minute values of the line flow            
                cur.execute("""SELECT val from minute_vals where val_type = 'inter_regional' and entity=%s order by min_num asc""", (cktName,))
                rows = cur.fetchall()
                if(len(rows) != 1440):
                    continue            
                for k in range(len(rows)):
                    minVals[k] = minVals[k] -1*rows[k][0]
                    cktMinVals[k] = -1*rows[k][0]
                # check if the ckt has already import mu defined in manual
                if (cktName in IRElineImpDict) and IRElineImpDict[cktName] != '':
                    import_mu = import_mu + float(IRElineImpDict[cktName])
                else:
                    # calculate import mu values from cktMinVals
                    cktImportVals = [(lambda x: (0 if x<0 else x))(x) for x in cktMinVals]
                    import_mu = import_mu + sum(cktImportVals)/60000                    
                # check if the ckt has already export mu defined in manual
                if (cktName in IRElineExpDict) and IRElineExpDict[cktName] != '':
                    export_mu = export_mu + float(IRElineExpDict[cktName])
                else:
                    # calculate export mu values from cktMinVals
                    cktExportVals = [(lambda x: (0 if x>0 else x))(x) for x in cktMinVals]
                    export_mu = export_mu + sum(cktExportVals)/60000                    
            lineSummaryDict['export_mu'] = export_mu
            lineSummaryDict['import_mu'] = import_mu
            # find the max_export (+ve values) and make save the result as negative
            exportVals = [(lambda x: (0 if x>0 else x))(x) for x in minVals]
            max_export = min(exportVals)
            # export_mu = sum(exportVals)/60000
            max_export_hrs = convert_min_to_time_str(exportVals.index(max_export))
            importVals = [(lambda x: (0 if x<0 else x))(x) for x in minVals]
            max_import = max(importVals)
            # import_mu = sum(importVals)/60000
            max_import_hrs = convert_min_to_time_str(importVals.index(max_import))
            lineSummaryDict['max_export'] = max_export
            
            lineSummaryDict['max_export_hrs'] = max_export_hrs
            lineSummaryDict['max_import'] = max_import
            lineSummaryDict['max_import_hrs'] = max_import_hrs
            lineSummaryDict['peak_flow_mw'] = minVals[peak_min]
            lineSummaryDict['off_peak_flow_mw'] = minVals[off_peak_min]
            # push the path summary values to the tuples
            for keyStr in lineSummaryDict.keys():
                tuples.append(dict(val_key=keyStr, entity=lineName, val=lineSummaryDict[keyStr]))
            lineFlowDict[lineName] = lineSummaryDict
        # push all the tuples to db
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
            
# transformation step #7
def transformGenDCSchDataAndPush():
    # for each wbes gen calculate wbes_peak_hr_mw, wbes_sch_mu, wbes_sch_peak_hr_mw, wbes_sch_mu
    try:
        conn = getConn()
        cur = conn.cursor()
        # find the peak hour
        cur.execute("""SELECT val from key_vals where val_key = %s AND entity = %s""", ('peak_hrs', 'config'))
        rows = cur.fetchall()
        peak_blk_Ind = int(float(rows[0][0]))*4
        # get the list of generators for dc info
        cur.execute("""SELECT distinct sch_type from blk_vals where entity = 'dc'""")
        rows = cur.fetchall()
        dcGenList = []
        for row in rows:
            dcGenList.append(row[0])
        dcGenSchDict = {}
        tuples = []
        for gen in dcGenList:
            dcSchDict = dict(wbes_dc_peak_hr_mw=0, wbes_dc_mu=0)
            dcGenSchDict[gen] = dcSchDict
            dcSchVals = []
            # get stoa vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", (gen, 'dc'))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                dcSchVals.append(row[0])
            dcSchDict['wbes_dc_peak_hr_mw'] = dcSchVals[peak_blk_Ind]
            dcSchDict['wbes_dc_mu'] = sum(dcSchVals)/4000
            for keyStr in dcSchDict.keys():
                tuples.append(dict(val_key=keyStr, entity=gen, val=dcSchDict[keyStr]))
            dcGenSchDict[gen] = dcSchDict
        
        # get the list of generators for inj_sch info
        cur.execute("""SELECT distinct sch_type from blk_vals where entity = 'inj_sch'""")
        rows = cur.fetchall()
        InjSchGenList = []
        for row in rows:
            InjSchGenList.append(row[0])
        InjSchGenDict = {}
        for gen in InjSchGenList:
            InjSchDict = dict(wbes_sch_peak_hr_mw=0, wbes_sch_mu=0)
            InjSchGenDict[gen] = InjSchDict
            schVals = []
            # get stoa vals
            cur.execute("""SELECT val from blk_vals where sch_type = %s AND entity = %s order by blk asc""", (gen, 'inj_sch'))
            rows = cur.fetchall()
            if(len(rows) != 96):
                continue
            for row in rows:
                schVals.append(row[0])
            InjSchDict['wbes_sch_peak_hr_mw'] = schVals[peak_blk_Ind]
            InjSchDict['wbes_sch_mu'] = sum(schVals)/4000
            for keyStr in InjSchDict.keys():
                tuples.append(dict(val_key=keyStr, entity=gen, val=InjSchDict[keyStr]))
            InjSchGenDict[gen] = InjSchDict
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

def convert_min_to_time_str(minReq):
    hrs = math.floor(minReq/60)
    mins = minReq - hrs*60
    return '{0}:{1}'.format(str(int(hrs)).zfill(2), str(int(mins)).zfill(2))

def convert_blk_to_time_strs(blk):
    startMins = (blk-1)*15
    endMins = blk*15
    return [convert_min_to_time_str(startMins), convert_min_to_time_str(endMins)]

def getAllKeyValsExceptConfig():
    try:
        conn = getConn()
        cur = conn.cursor()
        cur.execute("""SELECT entity, val_key, val from key_vals where entity<>'config' order by entity asc""")
        rows = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        return rows
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()
    
def push_report_vals_to_db(wb, sheet_name):
    try:
        conn = getConn()
        cur = conn.cursor()
        tuples = []
        valsArr= wb.sheets[sheet_name].range('A1').options(expand='table').value
        for valRow in valsArr:
            #stub
            [entity, val_key] = valRow[0].split('|')
            val = "" if valRow[1] == None else valRow[1]
            tuples.append(dict(val_key=val_key, entity=entity, val=val))
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
            page_size = 1000
        )
        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        print(e)
    finally:
        conn.close()

def pushLineDataToDB(wb, sheet_name):
    try:
        conn = getConn()
        cur = conn.cursor()
        tuples = []
        valsArr= wb.sheets[sheet_name].range('A1').options(expand='table').value
        for valRow in valsArr:
            line_name = valRow[0]
            ckt_names = valRow[1]
            region = valRow[2]
            if line_name != None:
                ckt_names = "" if ckt_names == None else ckt_names
                region = "" if region == None else region
                tuples.append(dict(line_name=line_name, ckt_names=ckt_names, region=region))
        # push all the tuples to db
        tuples_write = """
            insert into line_details (
                line_name,
                ckt_names,
                region
            ) values %s on conflict(line_name) do update set ckt_names = EXCLUDED.ckt_names, region = EXCLUDED.region
        """
        # push tuples to db
        execute_values (
            cur,
            tuples_write,
            tuples,
            template = """(
                %(line_name)s,
                %(ckt_names)s,
                %(region)s
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