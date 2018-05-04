import xlwings as xw
import datetime
import pandas as pd
import revs_helper
import wbes_helper
import scada_files_helper
import ids_helper

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
    

    
# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')

# paste_sch_dfs(wb)
# wb.close()