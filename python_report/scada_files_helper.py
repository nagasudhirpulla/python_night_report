# -*- coding: utf-8 -*-
"""
Created on Fri May 04 08:53:27 2018

@author: Nagasudhir
"""
import pandas as pd
import ids_helper
import xlwings as xw
import os
import math
try:
    from itertools import izip as zip
except ImportError: # will be 3.x series
    pass

def get_file_info_df():
    return pd.DataFrame(columns=['volt', 'gen_raw', 'state_raw', 'state_gen', 'inter_regional'], index=['config_key', 'type'], data=[['volt_filename', 'gen_raw_filename', 'state_raw_filename', 'state_gen_filename', 'inter_regional_filename'], ['volt', 'gen_raw', 'state_raw', 'state_gen', 'inter_regional']])
    
def get_scada_df(filename, fileType):
    if(filename.endswith('.csv')):
        df = pd.read_csv(filename, error_bad_lines=False)
    else:
        df = pd.read_excel(filename)
    k = pd.DataFrame(columns=df.columns, data=[[fileType for varr in df.columns]])
    df = k.append(df, ignore_index=True)
    if(fileType in ['volt', 'gen_raw', 'state_raw', 'state_gen']):
        df.columns = df.iloc[df.index[2]]
        df.drop(df.index[[1,2,df.index[-1]]], inplace=True)
        df.reset_index(drop=True, inplace=True)    
    return df

def paste_scada_df_wb(wb):
    config_df = ids_helper.get_config_df(wb)
    fileInfoDF = get_file_info_df()
    agg_df = pd.DataFrame()
    for col in fileInfoDF.columns:
        filename = config_df.loc[fileInfoDF.loc['config_key'][col]]['value']
        filename = os.path.dirname(wb.fullname).replace('\\', '/') + '/' + filename
        df = get_scada_df(filename, col)
        agg_df = pd.concat([agg_df, df], axis=1)
    wb.sheets['SCADA'].range('A1').value = agg_df
    
def get_blk_val(wb, nameStr, blk):
    config_df = ids_helper.get_config_df(wb)
    # wb.sheets['SCH'].range('A1').value = config_df
    headersArr= wb.sheets['SCADA'].range('A1').options(expand='right').value    
    blkVal = -1
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstMinRow = int(config_df.loc['scada_first_min_row']['value'])
        startMin = (blk - 1) * 15
        endMin = startMin + 14
        startRowIndex = startMin + firstMinRow - 1
        endRowIndex = endMin + firstMinRow - 1
        minVals = wb.sheets['SCADA'].range((startRowIndex+1, nameIndex+1), (endRowIndex+1,nameIndex+1)).value
        blkVal = sum(minVals) / len(minVals)
    return blkVal

def get_all_blk_vals(wb, nameStr):
    config_df = ids_helper.get_config_df(wb)
    # wb.sheets['SCH'].range('A1').value = config_df
    headersArr= wb.sheets['SCADA'].range('A1').options(expand='right').value    
    blkVals = []
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstMinRow = int(config_df.loc['scada_first_min_row']['value'])
        startMin = 0
        endMin = 1439
        startRowIndex = startMin + firstMinRow - 1
        endRowIndex = endMin + firstMinRow - 1
        minVals = wb.sheets['SCADA'].range((startRowIndex+1, nameIndex+1), (endRowIndex+1,nameIndex+1)).value
        for blk in range(1,97):
            blkMinVals = minVals[ ((blk - 1) * 15) : (blk * 15) ]
            blkVals.append(sum(blkMinVals)/len(blkMinVals))
    return blkVals

def get_max_blk_val(wb, nameStr):
    blkVals = get_all_blk_vals(wb, nameStr)
    maxBlkVal = max(blkVals)
    return maxBlkVal

def get_max_blk_val_blk(wb, nameStr):
    blkVals = get_all_blk_vals(wb, nameStr)
    maxBlkValBlk = blkVals.index(max(blkVals)) + 1
    return maxBlkValBlk

def get_min_blk_val(wb, nameStr):
    blkVals = get_all_blk_vals(wb, nameStr)
    minBlkVal = min(blkVals)
    return minBlkVal

def get_min_blk_val_blk(wb, nameStr):
    blkVals = get_all_blk_vals(wb, nameStr)
    minBlkValBlk = blkVals.index(min(blkVals)) + 1
    return minBlkValBlk

def get_minute_val(wb, nameStr, minReq):
    config_df = ids_helper.get_config_df(wb)
    # wb.sheets['SCH'].range('A1').value = config_df
    headersArr= wb.sheets['SCADA'].range('A1').options(expand='right').value    
    minVal = -1
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstMinRow = int(config_df.loc['scada_first_min_row']['value'])
        rowIndex = minReq + firstMinRow - 1
        minVal = wb.sheets['SCADA'].cells(rowIndex+1,nameIndex+1).value        
    return minVal

def get_all_minute_vals(wb, nameStr):
    config_df = ids_helper.get_config_df(wb)
    headersArr= wb.sheets['SCADA'].range('A1').options(expand='right').value    
    minVals = []
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstMinRow = int(config_df.loc['scada_first_min_row']['value'])
        startRowIndex = firstMinRow - 1
        endRowIndex = 1439 + firstMinRow - 1
        minVals = wb.sheets['SCADA'].range((startRowIndex+1,nameIndex+1), (endRowIndex+1,nameIndex+1)).value
    return minVals
    
def get_max_minute_val(wb, nameStr):
    minuteVals = get_all_minute_vals(wb, nameStr)
    maxVal = max(minuteVals)
    return maxVal

def get_max_minute_val_minute(wb, nameStr):
    minuteVals = get_all_minute_vals(wb, nameStr)
    maxValMinute = minuteVals.index(max(minuteVals))
    return maxValMinute

def get_min_minute_val(wb, nameStr):
    minuteVals = get_all_minute_vals(wb, nameStr)
    minVal = min(minuteVals)
    return minVal

def get_min_minute_val_minute(wb, nameStr):
    minuteVals = get_all_minute_vals(wb, nameStr)
    minValMinute = minuteVals.index(min(minuteVals))
    return minValMinute

def get_mu_val(wb, nameStr):
    return get_avg_val(wb, nameStr)*.024

def get_avg_val(wb, nameStr):
    minuteVals = get_all_minute_vals(wb, nameStr)
    avgVal = sum(minuteVals)/len(minuteVals)
    return avgVal

def get_export_mu_val(wb, nameStrs):
    minuteVals = get_scada_minute_vals_mul_col(wb, nameStrs.split('|'))
    exportVals = [(0 if x > 0 else x) for x in minuteVals]
    exportMU = sum(exportVals)*0.024/len(exportVals)
    return exportMU

def get_import_mu_val(wb, nameStrs):
    minuteVals = get_scada_minute_vals_mul_col(wb, nameStrs.split('|'))
    importVals = [(0 if x < 0 else x) for x in minuteVals]
    importMU = sum(importVals)*0.024/len(importVals)
    return importMU

def get_scada_val_less_than_prec(wb, nameStr, val):
    minuteVals = get_all_minute_vals(wb, nameStr)
    percVal = sum([1 if x < val else 0 for x in minuteVals])*100.0/len(minuteVals)
    return percVal

def get_scada_val_greater_than_prec(wb, nameStr, val):
    minuteVals = get_all_minute_vals(wb, nameStr)
    percVal = sum([1 if x > val else 0 for x in minuteVals])*100.0/len(minuteVals)
    return percVal

def get_scada_val_between_prec(wb, nameStr, valLow, valHigh):
    minuteVals = get_all_minute_vals(wb, nameStr)
    percVal = sum([1 if (x >= valLow and x <= valHigh) else 0 for x in minuteVals])*100.0/len(minuteVals)
    return percVal

def convert_min_to_time_str(minReq):
    hrs = math.floor(minReq/60)
    mins = minReq - hrs*60
    return '{0}:{1}'.format(str(int(hrs)).zfill(2), str(int(mins)).zfill(2))

def convert_blk_to_time_strs(blk):
    startMins = (blk-1)*15
    endMins = blk*15
    return [convert_min_to_time_str(startMins), convert_min_to_time_str(endMins)]

def get_scada_minute_vals_mul_col(wb, nameStrs):
    minuteValsArr = []
    for col in nameStrs:
        minuteValsArr.append(get_all_minute_vals(wb, col))
    minuteVals = [sum(x) for x in zip(*minuteValsArr)]
    return minuteVals
    
def get_ire_mw_at(wb, scadaStrs, minute):
    scadaMinuteVals = get_scada_minute_vals_mul_col(wb, scadaStrs.split('|'))
    return scadaMinuteVals[minute]

def get_max_import(wb, scada_mul, scadaStrs):
    scadaMinuteVals = get_scada_minute_vals_mul_col(wb, scadaStrs.split('|'))
    importVals = [(scada_mul*x) for x in scadaMinuteVals]
    importVals = [(0 if x<0 else x) for x in importVals]
    return max(importVals)

def get_max_export(wb, scada_mul, scadaStrs):
    scadaMinuteVals = get_scada_minute_vals_mul_col(wb, scadaStrs.split('|'))
    importVals = [(scada_mul*x) for x in scadaMinuteVals]
    importVals = [(0 if x>0 else x) for x in importVals]
    return min(importVals)

# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# paste_scada_df_wb(wb)
# df = get_scada_df('scada_files/Volt.xlsx', 'volt')
# df = get_scada_df('scada_files/State_Raw.xlsx', 'state_raw')
# df = get_scada_df('scada_files/StateGen.xlsx', 'state_gen')
