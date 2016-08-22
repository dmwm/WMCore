#! /usr/bin/env python
"""
Unit testing base class that turns on emulators
"""

from __future__ import (division, print_function)

import unittest

import mock

from WMCore.Services.SiteDB.SiteDBAPI import SiteDBAPI
from WMQuality.Emulators.DBSClient.MockDbsApi import MockDbsApi
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import MockPhEDExApi
from WMQuality.Emulators.SiteDBClient.MockSiteDBApi import mockGetJSON


class EmulatedUnitTestCase(unittest.TestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.
    """

    def __init__(self, methodName='runTest', mockDBS=True, mockPhEDEx=True, mockSiteDB=True):
        self.mockDBS = mockDBS
        self.mockPhEDEx = mockPhEDEx
        self.mockSiteDB = mockSiteDB

        self.mockingDBS = False
        self.mockingPhEDEx = False
        super(EmulatedUnitTestCase, self).__init__(methodName)

    def setUp(self):
        """
        Start the various mocked versions and add cleanups in case of exceptions

        TODO: parameters to turn off emulators individually
        """


        if self.mockDBS:
            for name in ['dbs.apis.dbsClient.DbsApi', 'WMCore.Services.DBS.DBS3Reader.DbsApi']:
                try:
                    dbsPatcher = mock.patch(name, new=MockDbsApi)
                    dbsPatcher.start()
                    self.addCleanup(dbsPatcher.stop)
                    self.mockingDBS = True
                except AttributeError:
                    print('Failure mocking DBS at %s. Continuing.' % name)
                    raise

        if self.mockPhEDEx:
            for name in ['WMCore.Services.PhEDEx.PhEDEx.PhEDEx', 'WMCore.WorkQueue.WorkQueue.PhEDEx',
                         'WMCore.Services.DBS.DBS3Reader.PhEDEx']:
                try:
                    phedexPatcher = mock.patch(name, new=MockPhEDExApi)
                    phedexPatcher.start()
                    self.addCleanup(phedexPatcher.stop)
                    self.mockingPhEDEx = True
                except AttributeError:
                    print('Failure mocking PhEDEx at %s. Continuing.' % name)
                    raise

        if self.mockSiteDB:
            self.siteDBPatcher = mock.patch.object(SiteDBAPI, 'getJSON', new=mockGetJSON)
            self.inUseSiteDBApi = self.siteDBPatcher.start()
            self.addCleanup(self.siteDBPatcher.stop)

        return
