# -*- coding: utf-8 -*-
"""
Created on Fri May 04 08:53:27 2018

@author: Nagasudhir
"""
import pandas as pd
import ids_helper
import xlwings as xw
import os

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
    

# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# paste_scada_df_wb(wb)
# df = get_scada_df('scada_files/Volt.xlsx', 'volt')
# df = get_scada_df('scada_files/State_Raw.xlsx', 'state_raw')
# df = get_scada_df('scada_files/StateGen.xlsx', 'state_gen')
