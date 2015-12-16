#! /usr/bin/env python
"""
Version of dbsClient.dbsApi intended to be used with mock or unittest.mock
"""

from __future__ import (division, print_function)

import json
import os

from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from WMCore.WMBase import getTestBase


# Read in the data just once so that we don't have to do it for every test (in __init__)

mockData = {}
globalFile = os.path.join(getTestBase(), '..', 'data', 'Mock', 'DBSMockData.json')
phys03File = os.path.join(getTestBase(), '..', 'data', 'Mock', 'DBSMockData03.json')

print (globalFile)

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

mockData['https://cmsweb.cern.ch/dbs/prod/global/DBSReader'] = mockDataGlobal
mockData['https://cmsweb.cern.ch/dbs/prod/phys03/DBSReader'] = mockData03


class MockDbsApi(object):
    def __init__(self, url):
        self.url = url.strip('/')

        # print("Initializing MockDBSApi")

    def serverinfo(self):
        return None

    def __getattr__(self, item):
        """
        __getattr__ gets called in case lookup of the actual method fails. We use this to return data based on
        a lookup table

        :param item: The method name the user is trying to call
        :return: The generic lookup function
        """

        def genericLookup(*args, **kwargs):
            """
            This function returns the mocked DBS data

            :param args: positional arguments it was called with
            :param kwargs: named arguments it was called with
            :return: the dictionary that DBS would have returned
            """
            if kwargs:
                signature = '%s:%s' % (item, sorted(kwargs.iteritems()))
            else:
                signature = item

            try:
                if mockData[self.url][signature] == 'Raises HTTPError':
                    raise HTTPError
                else:
                    return mockData[self.url][signature]
            except KeyError:
                raise KeyError("DBS mock API could not return data for method %s, args=%s, and kwargs=%s (URL %s)." %
                               (item, args, kwargs, self.url))

        return genericLookup
