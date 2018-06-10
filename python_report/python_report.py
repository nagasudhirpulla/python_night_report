import xlwings as xw
import datetime
import pandas as pd
import revs_helper
import wbes_helper
import scada_files_helper
import state_files_helper
import ids_helper
import db_helper
import re
import numpy as np

try:
    from itertools import izip as zip
except ImportError: # will be 3.x series
    pass

def hello_xlwings():
    wb = xw.Book.caller()
    wb.sheets[0].range("A1").value = "Hello xlwings!"


@xw.func
def hello(name):
    return "hello {0}".format(name)

@xw.func
def push_all_data_to_db():
    wb = xw.Book.caller()
    db_helper.push_config_to_db(wb)
    db_helper.push_sch_to_db(wb)
    db_helper.push_scada_to_db(wb)
    db_helper.push_hourly_to_db(wb)
    db_helper.push_key_vals_to_db(wb)
    db_helper.push_ire_manual_to_db(wb)
    
@xw.func
def push_config_to_db():
    wb = xw.Book.caller()
    db_helper.push_config_to_db(wb)
    
@xw.func
def push_constituents_db():
    db_helper.push_constituents_db()

@xw.func
def push_volt_config_to_db():
    wb = xw.Book.caller()
    db_helper.push_volt_config_to_db(wb)
    
@xw.func
def push_sch_to_db():
    wb = xw.Book.caller()
    db_helper.push_sch_to_db(wb)
    
@xw.func
def push_scada_to_db():
    wb = xw.Book.caller()
    db_helper.push_scada_to_db(wb)
    
@xw.func
def push_hourly_to_db():
    wb = xw.Book.caller()
    db_helper.push_hourly_to_db(wb)
    
@xw.func
def push_key_vals_to_db():
    wb = xw.Book.caller()
    db_helper.push_key_vals_to_db(wb)

@xw.func
def push_ire_manual_to_db():
    wb = xw.Book.caller()
    db_helper.push_ire_manual_to_db(wb)
    
@xw.func
def transform_raw_data_db():
    db_helper.transformRawData()

@xw.func
def dump_key_val_data():
    wb = xw.Book.caller()
    sheet_name = 'REPORT_VALS'
    rows = db_helper.getAllKeyValsExceptConfig()
    # convert the tuples list into a list of arrays and also combine entity and val_key as entity_val_key
    rows = [[row[0]+'|'+row[1], row[2]] for row in rows]
    # dump all the rows into the sheet
    wb.sheets[sheet_name].range('A1').value = rows
    
@xw.func
def push_modified_report_data():
    wb = xw.Book.caller()
    sheet_name = 'REPORT_VALS'
    db_helper.push_report_vals_to_db(wb, sheet_name)

@xw.func
def push_line_details_to_db():
    wb = xw.Book.caller()
    sheet_name = 'LINE_MAPPINGS'
    db_helper.pushLineDataToDB(wb, sheet_name)
    
@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
def latestRevForDate(baseURLStr, dateObj):
    return revs_helper.latestRevForDate(baseURLStr, dateObj)

@xw.func
def config_peak_hr():
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    peakHr = int(config_df.loc['peak_hrs']['value'])
    return peakHr

@xw.func
def config_peak_blk():
    peakHr = config_peak_hr()
    return 1 + 4*peakHr

@xw.func
def paste_sch_dfs():
    wb = xw.Book.caller()
    wbes_helper.paste_sch_dfs_wb(wb)
    
@xw.func
def paste_scada_dfs():
    wb = xw.Book.caller()
    scada_files_helper.paste_scada_df_wb(wb)
    
@xw.func
def paste_state_files():
    wb = xw.Book.caller()
    state_files_helper.paste_state_data_files(wb)

@xw.func
def freq_perc_between(rng, lowVal, highVal):
    cnt = 0.0;
    for freq in rng:
        if freq < highVal and freq > lowVal:
            cnt = cnt + 1
    return cnt*100/len(rng)

@xw.func
def freq_fvi(rng):
    fvi = 0;
    fvi = sum([(freq-50)**2 for freq in rng])
    fvi = fvi*10/len(rng)
    return fvi

@xw.func
def freq_quarterly_max(rng):
    q_max = 0;
    blkFreqs = []
    if len(rng) == 8640:
        for blkInd in range(96):
            blkFreqs.append(sum(rng[(blkInd*90):((blkInd+1)*90)])/90)
    q_max = max(blkFreqs)
    return q_max

@xw.func
def freq_quarterly_max_time(rng):
    q_max = 0;
    blkFreqs = []
    if len(rng) == 8640:
        for blkInd in range(96):
            blkFreqs.append(sum(rng[(blkInd*90):((blkInd+1)*90)])/90)
    q_max = max(blkFreqs)
    return scada_files_helper.convert_min_to_time_str((blkFreqs.index(q_max)+1)*4)

@xw.func
def freq_quarterly_min(rng):
    q_min = 0;
    blkFreqs = []
    if len(rng) == 8640:
        for blkInd in range(96):
            blkFreqs.append(sum(rng[(blkInd*90):((blkInd+1)*90)])/90)
    q_min = min(blkFreqs)
    return q_min

@xw.func
def freq_quarterly_min_time(rng):
    q_min = 0;
    blkFreqs = []
    if len(rng) == 8640:
        for blkInd in range(96):
            blkFreqs.append(sum(rng[(blkInd*90):((blkInd+1)*90)])/90)
    q_min = min(blkFreqs)
    return scada_files_helper.convert_min_to_time_str((blkFreqs.index(q_min)+1)*4)

@xw.func
def get_max_hourly_mul_rngs(rngStrs):
    wb = xw.Book.caller()
    return state_files_helper.get_max_hourly_mul_rngs(wb, rngStrs)

@xw.func
def get_max_hourly_hr_mul_rngs(rngStrs):
    wb = xw.Book.caller()
    return state_files_helper.get_max_hourly_hr_mul_rngs(wb, rngStrs)

@xw.func
def get_hourly_val_at_mul_rngs(rngStrs, hr):
    wb = xw.Book.caller()
    return state_files_helper.get_hourly_val_at_mul_rngs(wb, rngStrs, hr)

@xw.func
def get_max_pos(rng):
   return rng.index(max(rng)) + 1

@xw.func
def get_max_pos_2_col(rng1, rng2):
   rng = [a + b for a, b in zip(rng1, rng2)]
   return rng.index(max(rng)) + 1

@xw.func
def get_max_pos_3_col(rng1, rng2, rng3):
   rng = [a + b + c for a, b, c in zip(rng1, rng2, rng3)]
   return rng.index(max(rng)) + 1

@xw.func
def get_val_at_pos(rng, pos):
   # x is a DataFrame, do something with it
   return rng[int(pos)-1]

@xw.func
def get_min_pos(rng):
   # x is a DataFrame, do something with it
   return rng.index(min(rng)) + 1

@xw.func
def get_peak_val(rng):
   # x is a DataFrame, do something with it
   wb = xw.Book.caller()
   config_df = ids_helper.get_config_df(wb)
   peakHr = int(config_df.loc['peak_hrs']['value'])
   return rng[peakHr-1]
    
@xw.func
def extract_num(strng):
   # x is a DataFrame, do something with it
   s = re.search(r"\d+(\.\d+)?", strng)
   return int(s.group(0))

@xw.func
def get_sch_mu(stateStr):
    wb = xw.Book.caller()
    return wbes_helper.get_sch_mu(wb, stateStr)

@xw.func
def get_sch_blk_val(stateStr, blk):
    wb = xw.Book.caller()
    return wbes_helper.get_sch_blk_val(wb, int(blk), stateStr)

@xw.func
def get_sch_max_3_col(nameStr1, nameStr2, nameStr3):
    wb = xw.Book.caller()
    nameStrs = [nameStr1, nameStr2, nameStr3]
    return wbes_helper.get_sch_max_mul_col(wb, nameStrs)

@xw.func
def get_sch_min_3_col(nameStr1, nameStr2, nameStr3):
    wb = xw.Book.caller()
    nameStrs = [nameStr1, nameStr2, nameStr3]
    return wbes_helper.get_sch_min_mul_col(wb, nameStrs)

@xw.func
def get_sch_min(nameStr):
    wb = xw.Book.caller()
    return wbes_helper.get_sch_min(wb, nameStr)

@xw.func
def get_sch_max(nameStr):
    wb = xw.Book.caller()
    return wbes_helper.get_sch_max(wb, nameStr)

@xw.func
def get_scada_blk_val(nameStr, blk):
    wb = xw.Book.caller()
    return scada_files_helper.get_blk_val(wb, nameStr, int(blk))
   
@xw.func
def get_scada_max_blk_val(nameStr):
    wb = xw.Book.caller()
    return scada_files_helper.get_max_blk_val(wb, nameStr)

@xw.func
def get_scada_max_blk_val_time(nameStr):
    wb = xw.Book.caller()
    blk = scada_files_helper.get_max_blk_val_blk(wb, nameStr)
    return "-".join(scada_files_helper.convert_blk_to_time_strs(blk))
   
@xw.func
def get_scada_mu(nameStr):
    wb = xw.Book.caller()
    return scada_files_helper.get_mu_val(wb, nameStr)

@xw.func
def get_scada_max_val(nameStr):
    wb = xw.Book.caller()
    return scada_files_helper.get_max_minute_val(wb, nameStr)

@xw.func
def get_scada_max_val_time(nameStr):
    wb = xw.Book.caller()
    minute = scada_files_helper.get_max_minute_val_minute(wb, nameStr)
    return scada_files_helper.convert_min_to_time_str(minute)

@xw.func
def get_scada_min_val(nameStr):
    wb = xw.Book.caller()
    return scada_files_helper.get_min_minute_val(wb, nameStr)

@xw.func
def get_scada_min_val_time(nameStr):
    wb = xw.Book.caller()
    minute = scada_files_helper.get_min_minute_val_minute(wb, nameStr)
    return scada_files_helper.convert_min_to_time_str(minute)

@xw.func
def get_scada_avg(nameStr):
    wb = xw.Book.caller()
    return scada_files_helper.get_avg_val(wb, nameStr)

@xw.func
def get_scada_max_import(scada_mul, nameStrs):
    wb = xw.Book.caller()
    if not (nameStrs == None or nameStrs == ''):
        return scada_files_helper.get_max_import(wb, scada_mul, nameStrs)
    return 0
    

@xw.func
def get_scada_max_export(scada_mul, nameStrs):
    wb = xw.Book.caller()
    if not (nameStrs == None or nameStrs == ''):
        return scada_files_helper.get_max_export(wb, scada_mul, nameStrs)
    return 0

@xw.func
def get_scada_val_less_than_prec(nameStr, val):
    wb = xw.Book.caller()
    return scada_files_helper.get_scada_val_less_than_prec(wb, nameStr, val)
   
@xw.func
def get_scada_val_greater_than_prec(nameStr, val):
    wb = xw.Book.caller()
    return scada_files_helper.get_scada_val_greater_than_prec(wb, nameStr, val)
   
@xw.func
def get_ire_mw_at(scadaStrs,minute):
    wb = xw.Book.caller()
    if not (scadaStrs == None or scadaStrs == ''):
        return scada_files_helper.get_ire_mw_at(wb,scadaStrs,int(minute))
    return 0

@xw.func
def get_ire_import_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (ireStrs == None or ireStrs == ''):
        ireMU = state_files_helper.get_ire_import_mu(wb, ireStrs)
        if ireMU != None:
            return ireMU
    return 0

@xw.func
def get_ire_export_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (ireStrs == None or ireStrs == ''):
        ireMU = state_files_helper.get_ire_export_mu(wb, ireStrs)
        if ireMU != None:
            return ireMU    
    return 0

@xw.func
def get_scada_import_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (scadaStrs == None or scadaStrs == ''):
        return scada_files_helper.get_import_mu_val(wb, scadaStrs)
    return 0

@xw.func
def get_scada_export_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (scadaStrs == None or scadaStrs == ''):
        return scada_files_helper.get_export_mu_val(wb, scadaStrs)
    return 0

@xw.func
def get_ire_scada_import_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (ireStrs == None or ireStrs == ''):
        ireMU = state_files_helper.get_ire_import_mu(wb, ireStrs)
        if ireMU != None:
            return ireMU
    if not (scadaStrs == None or scadaStrs == ''):
        return scada_files_helper.get_import_mu_val(wb, scadaStrs)
    return None

@xw.func
def get_ire_scada_export_mu(ireStrs, scadaStrs):
    wb = xw.Book.caller()
    if not (ireStrs == None or ireStrs == ''):
        ireMU = state_files_helper.get_ire_export_mu(wb, ireStrs)
        if ireMU != None:
            return ireMU
    if not (scadaStrs == None or scadaStrs == ''):
        return scada_files_helper.get_export_mu_val(wb, scadaStrs)
    return None

@xw.func
def l1_state(stateStr, keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['state_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, keyStr, stateStr)
    
@xw.func
def l1_gen(genStr, keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['gen_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, genStr, keyStr)

@xw.func
def l1_volt(stationStr, keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['volt_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, stationStr, keyStr)

@xw.func
def l1_ire_data(lineStr, keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['ire_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, lineStr, keyStr)

@xw.func
def l1_ire_sch(pathStr, keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['ire_sch_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, pathStr, keyStr)

@xw.func
def l1_freq(keyStr):
    wb = xw.Book.caller()
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['freq_data_cell']['value']
    sheetName = 'LEVEL1_VALUES'
    return state_files_helper.get_table_val(wb, sheetName, headingCell, "value", keyStr)
    
# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')

# paste_sch_dfs(wb)
# wb.close()