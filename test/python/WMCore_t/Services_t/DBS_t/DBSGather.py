#!/usr/bin/env python
"""
_DBSGather_t_

Unit test for the DBS helper class.
"""

import pdb
import json

from WMCore.Services.DBS.DBSReader import DBSReader as DBSReader

lookup = {}

try:
    with open('DBSMockData.json', 'r') as mockData:
        lookup = json.load(mockData)
except IOError:
    lookup = {}

calls = [['listDataTiers']]
endpoint = 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'
dbs = DBSReader(endpoint)

for call in calls:
    func = getattr(dbs.wrapped.dbs, call[0])
    result = func()

    lookup.update({call[0]: result})


with open('DBSMockData.json', 'w') as mockData:
    json.dump(lookup, mockData)
