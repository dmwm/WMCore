#! /usr/bin/env python
"""
Unit testing base class that turns on emulators
"""

from __future__ import (division, print_function)

import unittest

import mock

from WMQuality.Emulators.DBSClient.MockDbsApi import MockDbsApi
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import MockPhEDExApi


class EmulatedUnitTest(unittest.TestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.

    FIXME: For now only DBS is mocked
    """

    def __init__(self, methodName='runTest', mockDBS=True, mockPhEDEx=False):  # FIXME: Default to False for both?
        self.mockDBS = mockDBS
        self.mockPhEDEx = mockPhEDEx
        super(EmulatedUnitTest, self).__init__(methodName)

    def setUp(self):
        """
        Start the various mocked versions and add cleanups in case of exceptions

        TODO: parameters to turn off emulators individually
        """

        if self.mockDBS:
            self.dbsPatcher = mock.patch('dbs.apis.dbsClient.DbsApi', new=MockDbsApi)
            self.inUseDbsApi = self.dbsPatcher.start()
            self.addCleanup(self.dbsPatcher.stop)

        if self.mockPhEDEx:
            self.phedexPatcher = mock.patch('WMCore.Services.PhEDEx.PhEDEx.PhEDEx', new=MockPhEDExApi)
            self.phedexPatcher2 = mock.patch('WMCore.WorkQueue.WorkQueue.PhEDEx', new=MockPhEDExApi)
            self.inUsePhEDExApi = self.phedexPatcher.start()
            self.phedexPatcher2.start()
            self.addCleanup(self.phedexPatcher.stop)
            self.addCleanup(self.phedexPatcher2.stop)

        return
