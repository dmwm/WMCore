#! /usr/bin/env python

"""
Version of SiteDB.SiteDBJSON intended to be used with mock or unittest.mock
"""
from __future__ import (division, print_function)

import os
import json
import logging
from WMCore.WMBase import getTestBase
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

# Read in the data just once so that we don't have to do it for every test (in __init__)
mockData = {}
globalFile = os.path.join(getTestBase(), '..', 'data', 'Mock', 'SiteDBMockData.json')
logging.debug("Reading mocked SiteDB data from the file %s " %globalFile)

try:
    with open(globalFile, 'r') as mockFile:
        mockData = json.load(mockFile)
except IOError:
    mockData = {}


def mockGetJSON(dummySelf, callname, filename='result.json', clearCache=False, verb='GET', data=None):
    """
    retrieve JSON formatted information using mock for the given call name and the
    argument dictionaries.

    """
    if data is None:
        data = {}

    #Build args
    args = {'callname':callname, 'filename':filename, 'clearCache':clearCache, 'verb':verb, 'data':data}
    result = {}
    signature = '%s' %(sorted(args.iteritems()))
    try:
        if mockData[signature] == 'Raises HTTPError':
            raise HTTPError
        else:
            return mockData[signature]
    except KeyError:
        raise KeyError("SiteDB mock API could not return data for the method: %s and args: %s" %(args['callname'], signature))

    return result
