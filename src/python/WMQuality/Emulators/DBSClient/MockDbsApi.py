#!/usr/bin/env python

"""
Version of dbsClient.dbsApi intended to be used with mock or unittest.mock
"""

from __future__ import (division, print_function)
from builtins import object
from future.utils import viewitems

import copy
import json
import os

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.Services.DBS.DBSErrors import DBSReaderError
from WMCore.WMBase import getTestBase

from Utils.Utilities import encodeUnicodeToBytesConditional
from Utils.PythonVersion import PY2

# Read in the data just once so that we don't have to do it for every test (in __init__)

mockData = {}
globalFile = os.path.join(getTestBase(), '..', 'data', 'Mock', 'DBSMockData.json')
phys03File = os.path.join(getTestBase(), '..', 'data', 'Mock', 'DBSMockData03.json')

try:
    with open(globalFile, 'r') as mockFile:
        mockDataGlobal = json.load(mockFile)
except IOError:
    mockDataGlobal = {}
try:
    with open(phys03File, 'r') as mockFile:
        mockData03 = json.load(mockFile)
except IOError:
    mockData03 = {}

mockData['https://cmsweb-prod.cern.ch/dbs/prod/global/DBSReader'] = mockDataGlobal
mockData['https://cmsweb-prod.cern.ch/dbs/prod/phys03/DBSReader'] = mockData03

class MockDbsApi(object):
    def __init__(self, url):
        print("Using MockDBSApi")
        self.url = url.strip('/')

        # print("Initializing MockDBSApi")

    def serverinfo(self):
        return {'dbs_instance': 'MOCK', 'dbs_version': '3.3.144'}

    def listFileArray(self, **kwargs):
        """
        Handle the case when logical_file_name is called with a list (longer than one) of files
        since we don't want to store all permutations. Rebuild the list of dicts that DBS returns

        Args:
            **kwargs: any kwargs that dbs client accepts

        Returns:

        """
        self.item = 'listFileArray'

        if 'logical_file_name' in kwargs and len(kwargs['logical_file_name']) > 1:
            origArgs = copy.deepcopy(kwargs)
            returnDicts = []
            for lfn in kwargs['logical_file_name']:
                origArgs.update({'logical_file_name': [lfn]})
                returnDicts.extend(self.genericLookup(**origArgs))
            return returnDicts
        else:
            return self.genericLookup(**kwargs)

    def listFileLumiArray(self, **kwargs):
        """
        Handle the case when logical_file_name is called with a list (longer than one) of files
        since we don't want to store all permutations. Rebuild the list of dicts that DBS returns

        Args:
            **kwargs: any kwargs that dbs client accepts

        Returns:

        """
        self.item = 'listFileLumiArray'

        if 'logical_file_name' in kwargs and len(kwargs['logical_file_name']) > 1:
            origArgs = copy.deepcopy(kwargs)
            returnDicts = []
            # since we iterate over this, we better make sure it's a list to avoid
            # things like: ['U', 'N', 'K', 'N', 'O', 'W', 'N']
            if isinstance(kwargs['logical_file_name'], str):
                kwargs['logical_file_name'] = [kwargs['logical_file_name']]
            for lfn in kwargs['logical_file_name']:
                origArgs.update({'logical_file_name': [lfn]})
                returnDicts.extend(self.genericLookup(**origArgs))
            return returnDicts
        else:
            return self.genericLookup(**kwargs)

    def __getattr__(self, item):
        """
        __getattr__ gets called in case lookup of the actual method fails. We use this to return data based on
        a lookup table

        :param item: The method name the user is trying to call
        :return: The generic lookup function
        """
        self.item = item
        return self.genericLookup

    def genericLookup(self, *args, **kwargs):
        """
        This function returns the mocked DBS data

        :param args: positional arguments it was called with
        :param kwargs: named arguments it was called with
        :return: the dictionary that DBS would have returned
        """
        if self.url not in mockData:
            raise DBSReaderError("Mock DBS emulator knows nothing about instance %s" % self.url)

        if kwargs:
            for k in kwargs:
                if isinstance(kwargs[k], (list, tuple)):
                    kwargs[k] = [encodeUnicodeToBytesConditional(item, condition=PY2) for item in kwargs[k]]
                else:
                    kwargs[k] = encodeUnicodeToBytesConditional(kwargs[k], condition=PY2)
            signature = '%s:%s' % (self.item, sorted(viewitems(kwargs)))
        else:
            signature = self.item

        try:
            if mockData[self.url][signature] == 'Raises HTTPError':
                raise HTTPError('http:/dbs.mock.fail', 400, 'MockDBS is raising an exception in place of DBS', 'Dummy header', 'Dummy body')
            else:
                return mockData[self.url][signature]
        except KeyError:
            if kwargs.get('dataset', None) == '/HighPileUp/Run2011A-v1/RAW-BLAH':
                return []
            raise KeyError("DBS mock API could not return data for method %s, args=%s, and kwargs=%s (URL %s) (Signature: %s)" %
                           (self.item, args, kwargs, self.url, signature))
