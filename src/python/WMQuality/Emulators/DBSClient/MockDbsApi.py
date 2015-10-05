#! /usr/bin/env python
"""
Version of dbsClient.dbsApi intended to be used with mock or unittest.mock
"""

from __future__ import (division, print_function)

import json

# Read in the data just once so that we don't have to do it for every test (in __init__)

try:
    with open('DBSMockData.json', 'r') as mockFile:
        mockData = json.load(mockFile)
except IOError:
    mockData = {}


class MockDbsApi(object):
    def __init__(self, url):
        self.url = url

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
            try:
                return mockData[item]
            except KeyError:
                print("DBS Mock called with method %s, args=%s, and kwargs=%s. No data returned" % (item, args, kwargs))

        return genericLookup

        # def listBlockParents(self, block_name=None):
        #     print('Calling mocked listBlockParents on block %s' % block_name)
        #     try:
        #         return self.mockData['listBlockParents']['block_name'][block_name]
        #     except (TypeError, AttributeError):
        #         raise RuntimeError('Data does not exist for block %s' % block_name)
        #
        # def listBlocks(self, block_name=None):
        #     print('Calling mocked listBlocks on block %s' % block_name)
        #     try:
        #         return self.mockData['listBlockParents']['block_name'][block_name]
        #     except (TypeError, AttributeError):
        #         raise RuntimeError('Data does not exist for block %s' % block_name)
