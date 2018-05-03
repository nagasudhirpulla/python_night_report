import xlwings as xw
import datetime
import pandas as pd
import revs_helper
import wbes_helper

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
@xw.arg('baseURLStr', doc='Base URL')
@xw.arg('dateObj', doc='Date String')
@xw.ret(index=False, header=True, expand='table')
def get_sch_dfs(baseURLStr, dateObj):
    rev = revs_helper.latestRevForDate(baseURLStr, dateObj)
    return pd.concat([wbes_helper.combine_all_state_dfs(baseURLStr, rev, dateObj), wbes_helper.get_isgs_dc_df(baseURLStr, rev, dateObj), wbes_helper.get_isgs_inj_df(baseURLStr, rev, dateObj), wbes_helper.get_flow_gate_sch_df(baseURLStr, rev, dateObj)], axis=1)

