#!/usr/bin/env python
"""
MakeDBSMockFiles

Program to create mock DBS JSON files used by the DBS mock-based emulator
"""

from __future__ import (division, print_function)

import json
import os

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.Services.DBS.DBSReader import DBSReader as DBSReader
from WMCore.WMBase import getTestBase
from WMQuality.Emulators.DBSClient.MockedDBSGlobalCalls import calls as calls00
from WMQuality.Emulators.DBSClient.MockedDBSGlobalCalls import datasets as datasets00
from WMQuality.Emulators.DBSClient.MockedDBSGlobalCalls import endpoint as endpoint00
from WMQuality.Emulators.DBSClient.MockedDBSphys03Calls import calls as calls03
from WMQuality.Emulators.DBSClient.MockedDBSphys03Calls import datasets as datasets03
from WMQuality.Emulators.DBSClient.MockedDBSphys03Calls import endpoint as endpoint03

instances = ((endpoint00, 'DBSMockData.json', calls00, datasets00),
             (endpoint03, 'DBSMockData03.json', calls03, datasets03))

for endpoint, outFile, calls, datasets in instances:
    lookup = {}
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', outFile)
    try:
        with open(outFilename, 'r') as mockData:
            lookup = json.load(mockData)
    except IOError:
        pass

    dbs = DBSReader(endpoint)
    realDBS = dbs.wrapped.dbs

    for dataset in datasets:
        calls.append(['listBlocks', {'detail': False, 'dataset': dataset}])
        calls.append(['listBlocks', {'detail': True, 'dataset': dataset}])
        calls.append(['listFileSummaries', {'validFileOnly': 1, 'dataset': dataset}])
        calls.append(['listRuns', {'dataset': unicode(dataset)}])
        blocks = realDBS.listBlocks(dataset=dataset)
        for block in blocks:
            calls.append(['listBlocks', {'block_name': unicode(block['block_name'])}])
            calls.append(['listBlocks', {'block_name': unicode(block['block_name']), 'detail': True}])
            calls.append(['listBlockParents', {'block_name': str(block['block_name'])}])
            calls.append(['listFileLumis', {'block_name': unicode(block['block_name'])}])
            calls.append(['listFileLumis', {'block_name': unicode(block['block_name']), 'validFileOnly': 1}])
            calls.append(['listFileArray', {'block_name': unicode(block['block_name']),
                                            'detail': True, 'validFileOnly': 1}])
            calls.append(['listFileSummaries', {'block_name': unicode(block['block_name']), 'validFileOnly': 1}])
            calls.append(['listFileSummaries', {'block_name': str(block['block_name']), 'validFileOnly': 1}])
            calls.append(['listRuns', {'block_name': unicode(block['block_name'])}])
            calls.append(['listFileParents', {'block_name': unicode(block['block_name'])}])
            files = realDBS.listFiles(block_name=block['block_name'])
            for dbsFile in files:
                lfn = unicode(dbsFile['logical_file_name'])
                calls.append(['listFileArray', {'logical_file_name': [lfn], 'detail': True}])
                calls.append(['listFileArray', {'logical_file_name': [lfn]}])
                calls.append(['listFileLumiArray', {'logical_file_name': [lfn]}])

    for call in calls:
        func = getattr(realDBS, call[0])
        if len(call) > 1:
            signature = '%s:%s' % (call[0], sorted(call[1].iteritems()))
            try:
                result = func(**call[1])
            except HTTPError:
                result = 'Raises HTTPError'
        else:
            result = func()
            signature = call[0]

        lookup.update({signature: result})

    with open(outFilename, 'w') as mockData:
        json.dump(lookup, mockData, indent=1, separators=(',', ': '))
