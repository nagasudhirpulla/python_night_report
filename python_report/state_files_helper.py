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
    df = pd.read_excel(filename, header=None, index_col=None)
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

def get_ire_val(wb, ireStrs, headingIndex):
    config_df = ids_helper.get_config_df(wb)
    headingCell = config_df.loc['ire_heading_cell']['value']
    ireValsArr = wb.sheets['IRE'].range(headingCell).options(expand='table').value
    ireColsList = wb.sheets['IRE'].range(headingCell).options(expand='down').value
    impMUVal = None
    for ireStr in ireStrs.split('|'):
        if ireStr in ireColsList:
            colIndex = ireColsList.index(ireStr)
            muVal = ireValsArr[colIndex][headingIndex]
            if muVal == None:
                continue
            muVal = float(muVal)
            impMUVal = (0 if impMUVal == None else impMUVal) + muVal
    return impMUVal

def get_ire_import_mu(wb, ireStrs):
    headingIndex = 3
    return get_ire_val(wb, ireStrs, headingIndex)

def get_ire_export_mu(wb, ireStrs):
    headingIndex = 4
    return get_ire_val(wb, ireStrs, headingIndex)    

def get_table_val(wb, sheetName, tableCornerAddr, rowStr, headStr):
        valsArr = wb.sheets[sheetName].range(tableCornerAddr).options(expand='table').value
        leftColList = wb.sheets[sheetName].range(tableCornerAddr).options(expand='down').value
        isColFound = False
        isRowFound = False
        
        if headStr in valsArr[0]:
            headingIndex = valsArr[0].index(headStr)
            isColFound = True
            
        if rowStr in leftColList:
            rowIndex = leftColList.index(rowStr)
            isRowFound = True
        
        if isColFound == True and isRowFound == True:
            return valsArr[rowIndex][headingIndex]
        else:
            return None
        
# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# paste_state_data_files(wb)
