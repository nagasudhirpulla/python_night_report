# -*- coding: utf-8 -*-
"""
Created on Sat May 05 19:25:17 2018

@author: Nagasudhir
"""
import os
import ids_helper
import pandas as pd
import xlwings as xw

def get_state_file_info_df():
    return pd.DataFrame(
            columns=['cseb', 'dd', 'dnh', 'esil', 'geb', 'goa', 'mp', 'mseb', 'ire'], 
            index=['config_key', 'sheet_name'], 
            data=[['cseb_filename', 'dd_filename', 'dnh_filename', 'esil_filename', 'geb_filename', 'goa_filename', 'mp_filename', 'mseb_filename', 'ire_filename'], 
                  ['CSEB', 'DD', 'DNH', 'ESIL', 'GEB', 'GOA', 'MP', 'MSEB', 'IRE']])

def get_state_df(filename, fileType):
    df = pd.read_excel(filename, header=None, index_col=None, dtype=str)
    return df

def paste_state_data_files(wb):
    config_df = ids_helper.get_config_df(wb)
    fileInfoDF = get_state_file_info_df()
    for col in fileInfoDF.columns:
        filename = config_df.loc[fileInfoDF.loc['config_key'][col]]['value']
        filename = os.path.dirname(wb.fullname).replace('\\', '/') + '/' + filename
        #df = get_state_df(filename, col)
        sheet_name = fileInfoDF.loc['sheet_name'][col]
        wbSrc = xw.Book(filename)
        endColIndex = 299
        endRowIndex = 299
        vals = wbSrc.sheets[0].range((1,1), (endRowIndex+1,endColIndex+1)).value
        wb.sheets[sheet_name].range('A1').value = vals


# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# paste_state_data_files(wb)
