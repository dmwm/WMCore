#! /usr/bin/env python

"""
Version of Services/CRIC intended to be used with mock or unittest.mock
"""
from __future__ import division, print_function

from builtins import object, str, bytes

import os
import json
import re
from WMCore.WMBase import getTestBase
from RestClient.ErrorHandling.RestClientExceptions import HTTPError

# Read in the data just once so that we don't have to do it for every test (in __init__)
mockData = {}
globalFile = os.path.join(getTestBase(), '..', 'data', 'Mock', 'CRICMockData.json')
print("Reading mocked CRIC data from the file %s" % globalFile)

try:
    with open(globalFile, 'r') as mockFile:
        mockData = json.load(mockFile)
except IOError:
    mockData = {}

class MockCRICApi(object):
    def __init__(self, url=None, logger=None, configDict=None):
        print("Using MockCRICApi")

    def genericLookup(self, callname):
        """
        This function returns the mocked CRIC data

        :param callname: the CRIC REST API name
        :return: the dictionary that CRIC would have returned
        """
        if callname not in mockData:
            raise RuntimeError("Mock CRIC emulator knows nothing about API %s" % callname)

        if mockData[callname] == 'Raises HTTPError':
            raise HTTPError('http:/cric.mock.fail', 400, 'MockCRIC is raising an exception in place of CRIC',
                            'Dummy header', 'Dummy body')
        else:
            return mockData[callname]

    def userNameDn(self, username):
        callname = 'people'
        res = self.genericLookup(callname)

        userdn = ""
        for x in res:
            if x['username'] == username:
                userdn = x['dn']
                break
        return userdn

    def getAllPSNs(self):
        callname = 'site-names'
        sitenames = self.genericLookup(callname)
        cmsnames = [x['alias'] for x in sitenames if x['type'] == 'psn']
        return cmsnames

    def getAllPhEDExNodeNames(self, pattern=None, excludeBuffer=False):
        callname = 'site-names'
        sitenames = self.genericLookup(callname)
        nodeNames = [x['alias'] for x in sitenames if x['type'] == 'phedex']
        if excludeBuffer:
            nodeNames = [x for x in nodeNames if not x.endswith("_Buffer")]
        if pattern and isinstance(pattern, (str, bytes)):
            pattern = re.compile(pattern)
            nodeNames = [x for x in nodeNames if pattern.match(x)]
        return nodeNames

    def PNNstoPSNs(self, pnns):
        callname = 'data-processing'
        mapping = self.genericLookup(callname)
        if isinstance(pnns, (str, bytes)):
            pnns = [pnns]

        psns = set()
        for pnn in pnns:
            psnSet = set()
            for item in mapping:
                if pnn == item['phedex_name']:
                    psnSet.add(item['psn_name'])
            if psnSet:
                psns.update(psnSet)
        return list(psns)

    def PSNstoPNNs(self, psns, allowPNNLess=False):
        callname = 'data-processing'
        mapping = self.genericLookup(callname)
        if isinstance(psns, (str, bytes)):
            psns = [psns]

        pnns = set()
        for psn in psns:
            pnnSet = set()
            for item in mapping:
                if item['psn_name'] == psn:
                    pnnSet.add(item['phedex_name'])
            if pnnSet:
                pnns.update(pnnSet)
            elif allowPNNLess:
                pnns.add(psn)
        return list(pnns)

    def PSNtoPNNMap(self, psnPattern=''):
        if not isinstance(psnPattern, (str, bytes)):
            raise TypeError('psnPattern argument must be of type str or bytes')

        callname = 'data-processing'
        results = self.genericLookup(callname)
        mapping = {}

        psnPattern = re.compile(psnPattern)
        for entry in results:
            if psnPattern.match(entry['psn_name']):
                mapping.setdefault(entry['psn_name'], set()).add(entry['phedex_name'])
        return mapping
