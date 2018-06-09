# -*- coding: utf-8 -*-
"""
Created on Fri Jun 08 22:59:13 2018

@author: Nagasudhir

https://stackoverflow.com/questions/2625877/how-to-copy-files-to-network-path-or-drive-using-python
https://stackoverflow.com/questions/4571244/creating-a-bat-file-for-python-script
"""
import os
import csv
import xlsxwriter
import ntpath
import shutil
from datetime import datetime
from datetime import timedelta

daysOffset = -1

reqDate = datetime.now() + timedelta(days=daysOffset)
reqDateStr = reqDate.strftime('_%d_%m_%Y')

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)

def saveCSVAsXlsx(csvFilename, destExcelFilename):
    # if we read f.csv we will write f.xlsx
    wb = xlsxwriter.Workbook(destExcelFilename)
    ws = wb.add_worksheet("Sheet1")    # your worksheet title here
    with open(csvFilename,'r') as csvfile:
        table = csv.reader(csvfile)
        i = 0
        # write each row from the csv file as text into the excel file
        # this may be adjusted to use 'excel types' explicitly (see xlsxwriter doc)
        for row in table:
            ws.write_row(i, 0, row)
            i += 1
    wb.close()

csvFilenames = [r"\\10.2.100.51\scada\Reports\Interregional\Interregional%s.xlsx" % reqDateStr, 
                r"\\10.2.100.51\scada\Reports\State_gen\STATE_gen%s.xlsx" % reqDateStr,
                r"\\10.2.100.51\scada\Reports\genschact\GENSCHACT%s.csv" % reqDateStr,
                r"\\10.2.100.51\scada\Reports\ONEMINREPORT\ONEMINREP_New%s.csv" % reqDateStr,
                r"\\10.2.100.51\scada\Reports\volttemp\VOLTTEMP%s.csv" % reqDateStr,
                ]

destFileFolder = r'scada_files'

# create folder if not present
if not os.path.exists(destFileFolder):
    os.makedirs(destFileFolder)

for iter, csvFilename in enumerate(csvFilenames):
    filename = path_leaf(csvFilename)
    if '.csv' in filename:
        destExcelFilename = filename.replace(".csv",".xlsx")
        destExcelFilePath = os.path.join(destFileFolder, destExcelFilename)
        saveCSVAsXlsx(csvFilename, destExcelFilePath)
    else:
        destExcelFilename = filename
        destExcelFilePath = os.path.join(destFileFolder, destExcelFilename)
        shutil.copy(csvFilename, destExcelFilePath)