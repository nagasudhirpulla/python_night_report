import xlwings as xw
import datetime
import pandas as pd
import revs_helper
import wbes_helper
import scada_files_helper
import state_files_helper
import ids_helper
import re

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
def get_scada_max_import(nameStrs):
    wb = xw.Book.caller()
    if not (nameStrs == None or nameStrs == ''):
        return scada_files_helper.get_max_import(wb, nameStrs)
    return 0
    

@xw.func
def get_scada_max_export(nameStrs):
    wb = xw.Book.caller()
    if not (nameStrs == None or nameStrs == ''):
        return scada_files_helper.get_max_export(wb, nameStrs)
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
        return scada_files_helper.get_ire_mw_at(wb, scadaStrs,int(minute))
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

# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')

# paste_sch_dfs(wb)
# wb.close()