# -*- coding: utf-8 -*-
"""
Created on Thu May 03 19:24:03 2018

@author: Nagasudhir
"""
import xlwings as xw
import numpy as np
import pandas as pd

def get_state_ids():
    idsObj = {}
    idsObj['cseb'] = 'c88b0ddb-e90c-4a89-8855-ac6512897c72'
    idsObj['dd'] = 'e881d351-afad-4034-9d22-53c6df736634'
    idsObj['dnh'] = 'b49743ce-b4cd-4f5e-bce9-bc6f406c93cd'
    idsObj['esil'] = '9d8d8acb-84ff-4516-8e84-88bc53585277'
    idsObj['geb'] = '2b2428f1-1992-40ef-ac36-d0dfbe843d04'
    idsObj['goa'] = '7fc7317c-89d8-4745-8e31-b865941cbd1a'
    idsObj['mp'] = '907e0643-1af6-4acd-8a19-ba0a414fbf04'
    idsObj['mseb'] = '82099353-d9b7-42f8-9e0e-2d5851fc59fa'
    return idsObj

def get_default_request_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
        'Accept-Encoding': 'gzip, deflate'
    }
    
def get_config_df(caller_wb):
    # http://docs.xlwings.org/en/stable/datastructures.html
    arr = caller_wb.sheets['config_params'].range('A1').options(expand='table').value
    df = pd.DataFrame(data = arr)
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    df.set_index('key', inplace=True)
    return df


# wb = xw.Book(r'C:/Users/Nagasudhir/Documents/Python Projects/Python Excel Reporting/python_report/python_report.xlsm')
# df = get_config_df(wb)
# wb.close()
# arr = wb.sheets['LEVEL_1'].range('D9').options(expand='table').value
