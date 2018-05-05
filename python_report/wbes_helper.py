# -*- coding: utf-8 -*-
"""
Created on Thu May 03 17:51:19 2018

@author: Nagasudhir
"""
from io import StringIO
# import unicodedata
import xlwings as xw
import requests
import datetime
import json
import revs_helper
import ids_helper
import pandas as pd
import itertools

def convert_csv_TextToDF(csvReadText):
    csvArray = [[val for val in lineStr.split(',')] for lineStr in csvReadText.decode("utf-16").split('\n')]
    df = pd.DataFrame(data=csvArray)
    return df

def get_state_csv_url(stateStr, baseURLStr, latestRev, dateObj):
    idsObj = ids_helper.get_state_ids()
    idStr = idsObj[str(stateStr)]
    fetchURL = '{0}/wbes/ReportNetSchedule/ExportNetScheduleSummaryToPDF?scheduleDate={1:%d-%m-%Y}&sellerId={2}&revisionNumber={3}&getTokenValue=1525350664894&fileType=csv&regionId=2&byDetails=1&isBuyer=1&isBuyer=1'.format(baseURLStr, dateObj, idStr, str(latestRev))
    return fetchURL

def get_state_net_sch_df(stateStr, baseURLStr, latestRev, dateObj):
    fetchURL = get_state_csv_url(stateStr, baseURLStr, latestRev, dateObj)
    # fetch state net sch
    r = requests.get(fetchURL, headers=ids_helper.get_default_request_headers())
    # check if we get a 200 ok response
    if r.status_code == requests.codes.ok:
        resText = r.text
        df = convert_csv_TextToDF(resText)
        # Make the first  row as header
        df = df.rename(columns=df.iloc[0])
        df.drop(['\r'], axis = 1, inplace = True)
        return df
    # if we dont get 200 ok response, send empty array
    return pd.DataFrame()

def get_state_csv_urls(baseURLStr, latestRev, dateObj):
    urlsObj = {}
    for stateStr in ids_helper.get_state_ids():
        fetchURL = get_state_csv_url(stateStr, baseURLStr, dateObj)
        urlsObj[stateStr] = fetchURL
    return urlsObj

def combine_all_state_dfs(baseURLStr, latestRev, dateObj):
    agg_df = pd.DataFrame()
    for stateStr in ids_helper.get_state_ids():
        df = get_state_net_sch_df(stateStr, baseURLStr, latestRev, dateObj)
        # add _statename to  each column
        df.columns=[str(col+'_'+stateStr) for col in df.columns]
        k = pd.DataFrame(columns=df.columns, data=[[stateStr for varr in df.columns]])
        df = k.append(df, ignore_index=True)
        agg_df = pd.concat([agg_df, df], axis=1)
    return agg_df

def get_isgs_inj_df(baseURLStr, latestRev, dateObj):
    fetchURL = '{0}/wbes/ReportFullSchedule/ExportFullScheduleInjSummaryToPDF?scheduleDate={1:%d-%m-%Y}&sellerId=ALL&revisionNumber={2}&getTokenValue=1525350664895&fileType=csv&regionId=2&byDetails=0&isDrawer=0&isBuyer=0'.format(baseURLStr, dateObj, latestRev)
    # fetch state net sch
    r = requests.get(fetchURL, headers=ids_helper.get_default_request_headers())
    # check if we get a 200 ok response
    if r.status_code == requests.codes.ok:
        resText = r.text
        df = convert_csv_TextToDF(resText)
        # Make the first  row as header
        df = df.rename(columns=df.iloc[0])
        df.drop(['\r'], axis = 1, inplace = True)
        # add _inj to  each column
        df.columns=[str(col+'_inj') for col in df.columns]
        k = pd.DataFrame(columns=df.columns, data=[['inj_sch' for varr in df.columns]])
        df = k.append(df, ignore_index=True)
        return df
    # if we dont get 200 ok response, send empty array
    return pd.DataFrame()

def get_isgs_dc_df(baseURLStr, latestRev, dateObj):
    fetchURL = '{0}/wbes/Report/ExportDeclarationRldcToPDF?scheduleDate={1:%d-%m-%Y}&getTokenValue=1525350664895&fileType=csv&Region=2&UtilId=ALL&Revision={2}&isBuyer=0&byOnBar=0'.format(baseURLStr, dateObj, latestRev)
    # fetch state net sch
    r = requests.get(fetchURL, headers=ids_helper.get_default_request_headers())
    # check if we get a 200 ok response
    if r.status_code == requests.codes.ok:
        resText = r.text
        df = convert_csv_TextToDF(resText)
        # Make the first  row as header
        df = df.rename(columns=df.iloc[0])
        df.drop(['\r'], axis = 1, inplace = True)
        trimmedHeaders = [str((col[:col.find('(')] if (col.find('(') != -1) else col)) for col in df.columns]
        # add _inj to  each column
        df.columns=[str(col + '_dc') for col in trimmedHeaders]
        df.drop(df.index[0], inplace = True)
        k = pd.DataFrame(columns=df.columns, data=[['dc' for varr in df.columns], [col for col in trimmedHeaders]])
        df = k.append(df, ignore_index=True)
        return df
    # if we dont get 200 ok response, send empty array
    return pd.DataFrame()

def get_flow_gate_sch_df(baseURLStr, latestRev, dateObj):
    fetchURL = '{0}/wbes/Report/ExportFlowGateScheduleToPDF?scheduleDate={1:%d-%m-%Y}&getTokenValue=1525350664895&fileType=csv&revisionNumber={2}&pathId=0&scheduleType=-1&isLink=1'.format(baseURLStr, dateObj, latestRev)
    # fetch state net sch
    r = requests.get(fetchURL, headers=ids_helper.get_default_request_headers())
    # check if we get a 200 ok response
    if r.status_code == requests.codes.ok:
        resText = r.text
        df = convert_csv_TextToDF(resText)
        df.columns=df.iloc[0]
        df.drop(['\r'], axis = 1, inplace = True)
        # replace header columns with combination of first and second rows as header
        # and remove first and second rows
        df.columns=df.iloc[0] + "_" + df.iloc[1]
        # df.drop(df.index[[0,1]], inplace=True)
        return df
    # if we dont get 200 ok response, send empty array
    return pd.DataFrame()

def get_sch_dfs(baseURLStr, dateObj, rev):
    return pd.concat([combine_all_state_dfs(baseURLStr, rev, dateObj), get_isgs_dc_df(baseURLStr, rev, dateObj), get_isgs_inj_df(baseURLStr, rev, dateObj), get_flow_gate_sch_df(baseURLStr, rev, dateObj)], axis=1)

def paste_sch_dfs_wb(wb):
    config_df = ids_helper.get_config_df(wb)
    # wb.sheets['SCH'].range('A1').value = config_df
    dateObj = config_df.loc['date']['value']
    baseURLStr = config_df.loc['baseURL']['value']
    rev = int(config_df.loc['Revision']['value'])
    sch_dfs = get_sch_dfs(baseURLStr, dateObj, rev)
    wb.sheets['SCH'].range('A1').value = sch_dfs
    
def get_sch_blk_vals(wb, nameStr):
    blkVals = []
    config_df = ids_helper.get_config_df(wb)
    # wb.sheets['SCH'].range('A1').value = config_df
    headersArr= wb.sheets['SCH'].range('A1').options(expand='right').value    
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstBlkRow = int(config_df.loc['sch_first_blk_row']['value'])
        startRowIndex = firstBlkRow - 1
        endRowIndex = 95 + firstBlkRow - 1
        blkVals = wb.sheets['SCH'].range((startRowIndex+1,nameIndex+1), (endRowIndex+1,nameIndex+1)).value
    return blkVals
    
def get_sch_avg(wb, nameStr):
    blkVals = get_sch_blk_vals(wb, nameStr)
    return sum(blkVals)/len(blkVals)

def get_sch_mu(wb, nameStr):
    return get_sch_avg(wb, nameStr)*0.024

def sch_blk_vals_mul_col(wb, nameStrs):
    blkValsArr = []
    for col in nameStrs:
        blkValsArr.append(get_sch_blk_vals(wb, col))
    # https://stackoverflow.com/questions/18713321/element-wise-addition-of-2-lists
    # [sum(x) for x in itertools.izip(*[[1,2,3], [4,5,6], [7,8,9]])] = [12, 15, 18]
    blkVals = [sum(x) for x in itertools.izip(*blkValsArr)]
    return blkVals


# x =  revs_helper.latestRevForDate("http://103.7.130.121", datetime.datetime.now())

# x = get_state_csv_url('cseb', "http://103.7.130.121", datetime.datetime.now())

# x = get_state_csv_urls("http://103.7.130.121", datetime.datetime.now())

# x = get_state_net_sch_df('dnh', "http://103.7.130.121", revs_helper.latestRevForDate("http://103.7.130.121", datetime.datetime.now()), datetime.datetime.now())

# x = combine_all_state_dfs("http://103.7.130.121", revs_helper.latestRevForDate("http://103.7.130.121", datetime.datetime.now()), datetime.datetime.now())

# x = get_isgs_inj_df("http://103.7.130.121", revs_helper.latestRevForDate("http://103.7.130.121", datetime.datetime.now()), datetime.datetime.now())
