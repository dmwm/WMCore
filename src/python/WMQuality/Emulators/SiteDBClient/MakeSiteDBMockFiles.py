#!/usr/bin/env python
"""
MakeSiteDBMockFiles
Program to create mock SiteDB JSON files used by the SiteDB mock-based emulator
"""

from __future__ import (division, print_function)
from urllib2 import HTTPError
import json
import os
from WMCore.Services.SiteDB.SiteDBAPI import SiteDBAPI
from WMCore.WMBase import getTestBase

if __name__ == '__main__':
    calls = [{'callname': 'people', 'filename': 'people.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'site-names', 'filename': 'site-names.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'site-resources', 'filename': 'site-resources.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'data-processing', 'filename': 'data-processing.json', 'clearCache': False, 'verb': 'GET', 'data':{}}
            ]

    dns = ["/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'",
           "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jha/CN=618566/CN=Manoj Jha"]
    lookup = {}

    outFile = 'SiteDBMockData.json'
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', outFile)
    print("Creating the file %s with mocked SiteDB data" %outFilename)

    siteDB = SiteDBAPI()
    for call in calls:
        signature = str(sorted(call.iteritems()))
        try:
            result = siteDB.getJSON(**call)
        except HTTPError:
            result = 'Raises HTTPError'

        if call['callname'] == 'people':
            result = [res  for res in result for dn in dns if res['dn'] == dn]
        lookup.update({signature:result})
    with open(outFilename, 'w') as mockData:
        json.dump(lookup, mockData, indent=1, separators=(',', ': '))
