#!/usr/bin/env python
"""
MakeSiteDBMockFiles
Program to create mock SiteDB JSON files used by the SiteDB mock-based emulator
"""

from __future__ import (division, print_function)

import json
import os
from urllib2 import HTTPError

from WMCore.Services.SiteDB.SiteDBAPI import SiteDBAPI
from WMCore.WMBase import getTestBase

if __name__ == '__main__':
    calls = [{'callname': 'people', 'filename': 'people.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'site-names', 'filename': 'site-names.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'site-resources', 'filename': 'site-resources.json', 'clearCache': False, 'verb': 'GET', 'data':{}},
             {'callname': 'data-processing', 'filename': 'data-processing.json', 'clearCache': False, 'verb': 'GET', 'data':{}}
            ]

    additionals = {
        'site-names': [
            {"site_name": "T2_XX_SiteA", "type": "psn", "alias": "T2_XX_SiteA"},
            {"site_name": "T2_XX_SiteA", "type": "cms", "alias": "T2_XX_SiteA"},
            {"site_name": "T2_XX_SiteA", "type": "phedex", "alias": "T2_XX_SiteA"},
            {"site_name": "T2_XX_SiteB", "type": "psn", "alias": "T2_XX_SiteB"},
            {"site_name": "T2_XX_SiteB", "type": "cms", "alias": "T2_XX_SiteB"},
            {"site_name": "T2_XX_SiteB", "type": "phedex", "alias": "T2_XX_SiteB"},
            {"site_name": "T2_XX_SiteC", "type": "psn", "alias": "T2_XX_SiteC"},
            {"site_name": "T2_XX_SiteC", "type": "cms", "alias": "T2_XX_SiteC"},
            {"site_name": "T2_XX_SiteC", "type": "phedex", "alias": "T2_XX_SiteC"},
        ],
        'data-processing': [
            {u'phedex_name': u'T2_XX_SiteA', u'psn_name': u'T2_XX_SiteA', u'site_name': u'XX_T2_XX_SiteA'},
            {u'phedex_name': u'T2_XX_SiteB', u'psn_name': u'T2_XX_SiteB', u'site_name': u'XX_T2_XX_SiteB'},
            {u'phedex_name': u'T2_XX_SiteC', u'psn_name': u'T2_XX_SiteC', u'site_name': u'XX_T2_XX_SiteC'}
        ]
    }

    dns = ["/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=liviof/CN=472739/CN=Livio Fano'",
           "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=jha/CN=618566/CN=Manoj Jha"]
    lookup = {}

    outFile = 'SiteDBMockData.json'
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', outFile)
    print("Creating the file %s with mocked SiteDB data" %outFilename)

    siteDB = SiteDBAPI()
    for call in calls:
        signature = str(sorted(call.iteritems()))
        callname = call['callname']
        try:
            result = siteDB.getJSON(**call)
        except HTTPError:
            result = 'Raises HTTPError'

        if callname == 'people':
            result = [res  for res in result for dn in dns if res['dn'] == dn]
        elif callname == 'site-names' or callname == 'data-processing':
            for additional in additionals[callname]:
                result.append(additional)
        else:
            result = result

        lookup.update({signature:result})
    with open(outFilename, 'w') as mockData:
        json.dump(lookup, mockData, indent=1, separators=(',', ': '), sort_keys=True)
