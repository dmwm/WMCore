#! /usr/bin/env python
"""
Version of dbsClient.dbsApi intended to be used with mock or unittest.mock
"""

from __future__ import (division, print_function)


class MockDbsApi(object):
    def __init__(self, url):
        self.url = url

        self.mockData = None  # json.load()

    def serverinfo(self):
        return None

    def listBlockParents(self, block_name=None):
        print('Calling mocked listBlockParents on block %s' % block_name)
        try:
            return self.mockData['listBlockParents']['block_name'][block_name]
        except (TypeError, AttributeError):
            raise RuntimeError('Data does not exist for block %s' % block_name)
