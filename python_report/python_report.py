import xlwings as xw
import datetime
import pandas as pd
import revs_helper
import wbes_helper
import scada_files_helper
import state_files_helper
import ids_helper
import re
from itertools import izip

def hello_xlwings():
    wb = xw.Book.caller()
    wb.sheets[0].range("A1").value = "Hello xlwings!"


@xw.func
def hello(name):
    return "hello {0}".format(name)
        
@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
def revsFetchURL(baseURLStr, dateObj):
    return revs_helper.revsFetchURL(baseURLStr, dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
def revsForDate(baseURLStr, dateObj):
    return revs_helper.revsForDate(baseURLStr, dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
def latestRevForDate(baseURLStr, dateObj):
    return revs_helper.latestRevForDate(baseURLStr, dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
@xw.ret(index=False, header=True, expand='table')
def state_dfs(baseURLStr, dateObj):
    return wbes_helper.combine_all_state_dfs(baseURLStr, revs_helper.latestRevForDate(baseURLStr, dateObj), dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
@xw.ret(index=False, header=True, expand='table')
def inj_sch_dfs(baseURLStr, dateObj):
    return wbes_helper.get_isgs_inj_df(baseURLStr, revs_helper.latestRevForDate(baseURLStr, dateObj), dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
@xw.ret(index=False, header=True, expand='table')
def dc_dfs(baseURLStr, dateObj):
    return wbes_helper.get_isgs_dc_df(baseURLStr, revs_helper.latestRevForDate(baseURLStr, dateObj), dateObj)

@xw.func
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
@xw.ret(index=False, header=True, expand='table')
def flow_gate_dfs(baseURLStr, dateObj):
    return wbes_helper.get_flow_gate_sch_df(baseURLStr, revs_helper.latestRevForDate(baseURLStr, dateObj), dateObj)

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
   rng = [a + b for a, b in izip(rng1, rng2)]
   return rng.index(max(rng)) + 1

@xw.func
def get_max_pos_3_col(rng1, rng2, rng3):
   rng = [a + b + c for a, b, c in izip(rng1, rng2, rng3)]
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
   
# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')

# paste_sch_dfs(wb)
# wb.close()