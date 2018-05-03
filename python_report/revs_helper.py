# -*- coding: utf-8 -*-
"""
Created on Thu May 03 18:56:10 2018

@author: Nagasudhir
"""

import requests
import requests
import datetime
import json

defaultRequestHeaders = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36',
    'Accept-Encoding': 'gzip, deflate'
}

def revsFetchURL(baseURLStr, dateObj):
    revisionsFetchUrl = "{0}/wbes/Report/GetNetScheduleRevisionNoForSpecificRegion?regionid=2&ScheduleDate={1:%d-%m-%Y}".format(baseURLStr, dateObj)
    return revisionsFetchUrl

def revsForDate(baseURLStr, dateObj):
    # get the url to fetch revision
    revisionsFetchUrl = revsFetchURL(baseURLStr, dateObj)
    # fetch revisions array
    r = requests.get(revisionsFetchUrl, headers=defaultRequestHeaders)
    # check if we get a 200 ok response
    if r.status_code == requests.codes.ok:
        resText = r.text
        revsArray = json.loads(resText)
        return revsArray
    # if we dont get 200 ok response, send empty array
    return []

def latestRevForDate(baseURLStr, dateObj):
    revs = revsForDate(baseURLStr, dateObj)
    if len(revs) > 0:
        return revs[0]
    else:
        return -1