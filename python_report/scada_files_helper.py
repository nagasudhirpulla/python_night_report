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

def get_file_info_df():
    return pd.DataFrame(columns=['volt', 'gen_raw', 'state_raw', 'state_gen', 'inter_regional'], index=['config_key', 'type'], data=[['volt_filename', 'gen_raw_filename', 'state_raw_filename', 'state_gen_filename', 'inter_regional_filename'], ['volt', 'gen_raw', 'state_raw', 'state_gen', 'inter_regional']])
    
def get_scada_df(filename, fileType):
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
        startMin = (blk - 1) * 15 + 1
        endMin = startMin + 14
        startRowIndex = startMin - 1 + firstMinRow - 1
        endRowIndex = endMin - 1 + firstMinRow - 1
        minVals = [wb.sheets['SCADA'].cells(rowIndex+1,nameIndex+1).value for rowIndex in range(startRowIndex, endRowIndex+1)]
        blkVal = sum(minVals) / len(minVals)
    return blkVal

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
    # wb.sheets['SCH'].range('A1').value = config_df
    headersArr= wb.sheets['SCADA'].range('A1').options(expand='right').value    
    minVals = []
    if(nameStr in headersArr):
        nameIndex = headersArr.index(nameStr)
        firstMinRow = int(config_df.loc['scada_first_min_row']['value'])
        startRowIndex = firstMinRow - 1
        endRowIndex = 1440 - 1 + firstMinRow - 1
        minVals = [wb.sheets['SCADA'].cells(rowIndex+1,nameIndex+1).value for rowIndex in range(startRowIndex, endRowIndex+1)]
    return minVals
    
def convert_min_to_time_str(minReq):
    hrs = math.floor(minReq/60)
    mins = minReq - hrs*60
    return '{0}:{1}'.format(str(int(hrs)).zfill(2), str(int(mins)).zfill(2))


# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# paste_scada_df_wb(wb)
# df = get_scada_df('scada_files/Volt.xlsx', 'volt')
# df = get_scada_df('scada_files/State_Raw.xlsx', 'state_raw')
# df = get_scada_df('scada_files/StateGen.xlsx', 'state_gen')
