# -*- coding: utf-8 -*-
"""
Created on Thu May 03 17:51:19 2018

@author: Nagasudhir
"""
import xlwings as xw
import requests
import datetime
import json
import revs_helper
import ids_helper
import db_helper as db
import psycopg2



def get_state_csv_url(stateStr, baseURLStr, dateObj):
    latestRev = revs_helper.latestRevForDate("http://103.7.130.121", dateObj)
    idsObj = ids_helper.get_state_ids()
    idStr = idsObj[str(stateStr)]
    fetchURL = '{0}/wbes/ReportNetSchedule/ExportNetScheduleSummaryToPDF?scheduleDate={1:%d-%m-%Y}&sellerId={2}&revisionNumber={3}&getTokenValue=1525350664894&fileType=csv&regionId=2&byDetails=1&isBuyer=1&isBuyer=1'.format(baseURLStr, dateObj, idStr, str(latestRev))
    return fetchURL

def get_state_csv_urls(baseURLStr, dateObj):
    latestRev = revs_helper.latestRevForDate("http://103.7.130.121", dateObj)
    urlsObj = {}
    for stateStr in ids_helper.get_state_ids():
        fetchURL = get_state_csv_url(stateStr, baseURLStr, dateObj)
        urlsObj[stateStr] = fetchURL
    return urlsObj

# valsArr= wb.sheets['VOLT_INFO'].range('A1').options(expand='table').value

# wb = xw.Book(r'python_report.xlsm')

# x =  revs_helper.latestRevForDate("http://103.7.130.121", datetime.datetime.now())

# x = get_state_csv_url('cseb', "http://103.7.130.121", datetime.datetime.now())

# x = get_state_csv_urls("http://103.7.130.121", datetime.datetime.now())