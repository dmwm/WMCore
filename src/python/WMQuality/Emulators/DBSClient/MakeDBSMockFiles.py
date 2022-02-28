#!/usr/bin/env python
"""
MakeDBSMockFiles

Program to create mock DBS JSON files used by the DBS mock-based emulator

If there are either dataset or dbs calls removal, then you need to create
new files from scratch (i.e., you need to delete the previous DBS json
mocked data).
"""

from __future__ import (division, print_function)

from future.utils import viewitems

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

INSTANCES = ((endpoint00, 'DBSMockData.json', calls00, datasets00),
             (endpoint03, 'DBSMockData03.json', calls03, datasets03))

for endpoint, outFile, calls, datasets in INSTANCES:
    lookup = {}
    outFilename = os.path.join(getTestBase(), '..', 'data', 'Mock', outFile)
    ## Instead of updating on top of the previous file, just go with a new one
    #try:
    #    with open(outFilename, 'r') as mockData:
    #        lookup = json.load(mockData)
    #except IOError:
    #    pass

    dbs = DBSReader(endpoint)
    realDBS = dbs.dbs

    for dataset in datasets:
        print("Building call list for", dataset)
        calls.append(['listDatasetParents', {'dataset': dataset}])
        calls.append(['listDatasets', {'dataset_access_type': '*', 'dataset': dataset}])
        calls.append(['listBlockOrigin', {'dataset': dataset}])
        calls.append(['listBlocks', {'dataset': dataset}])
        calls.append(['listBlocks', {'detail': False, 'dataset': dataset}])
        calls.append(['listBlocks', {'detail': True, 'dataset': dataset}])
        calls.append(['listFileSummaries', {'validFileOnly': 1, 'dataset': dataset}])
        calls.append(['listFileArray', {'dataset': dataset, 'detail': True, 'validFileOnly': 1}])
        calls.append(['listFileArray', {'dataset': dataset, 'detail': False, 'validFileOnly': 1}])
        calls.append(['listRuns', {'dataset': dataset}])
        blocks = realDBS.listBlocks(dataset=dataset)
        for block in blocks:
            calls.append(['listBlocks', {'block_name': block['block_name']}])
            calls.append(['listBlocks', {'block_name': block['block_name'], 'detail': True}])
            calls.append(['listBlockParents', {'block_name': str(block['block_name'])}])
            calls.append(['listFileLumis', {'block_name': block['block_name']}])
            calls.append(['listFileLumis', {'block_name': block['block_name'], 'validFileOnly': 1}])
            calls.append(['listFileArray', {'block_name': block['block_name'],
                                            'detail': True, 'validFileOnly': 1}])
            calls.append(['listFileSummaries', {'block_name': block['block_name'], 'validFileOnly': 1}])
            calls.append(['listRuns', {'block_name': block['block_name']}])
            calls.append(['listFileParents', {'block_name': block['block_name']}])
            files = realDBS.listFiles(block_name=block['block_name'])
            for dbsFile in files:
                lfn = dbsFile['logical_file_name']
                calls.append(['listFileArray', {'logical_file_name': [lfn], 'detail': True}])
                calls.append(['listFileArray', {'logical_file_name': [lfn]}])
                calls.append(['listFileLumiArray', {'logical_file_name': [lfn]}])

    nCalls = len(calls)
    print("Need to issue %d calls to DBS" % nCalls)
    callsDone = 0
    progress = [2.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05, 0.03, 0.02, 0.01]
    for call in calls:
        callsDone += 1
        if callsDone / nCalls > progress[-1]:
            percentDone = progress.pop() * 100
            print(" Fetching call list %d%% done." % percentDone)
        func = getattr(realDBS, call[0])
        if len(call) > 1:
            signature = '%s:%s' % (call[0], sorted(viewitems(call[1])))
            try:
                result = func(**call[1])
            except HTTPError:
                result = 'Raises HTTPError'
        else:
            result = func()
            signature = call[0]

        lookup.update({signature: result})

    print("Writing out %s file" % outFilename)
    with open(outFilename, 'w') as mockData:
        json.dump(lookup, mockData, indent=1, separators=(',', ': '), sort_keys=True)
