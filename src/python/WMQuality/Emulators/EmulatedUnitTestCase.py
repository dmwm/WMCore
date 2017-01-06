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
        super(EmulatedUnitTestCase, self).__init__(methodName)

    def setUp(self):
        """
        Start the various mocked versions and add cleanups in case of exceptions

        TODO: parameters to turn off emulators individually
        """

        if self.mockDBS:
            self.dbsPatcher1 = mock.patch('dbs.apis.dbsClient.DbsApi', new=MockDbsApi)
            self.dbsPatcher2 = mock.patch('WMCore.Services.DBS.DBS3Reader.DbsApi', new=MockDbsApi)
            self.inUseDbsApi = self.dbsPatcher1.start()
            self.inUseDbsApi = self.dbsPatcher2.start()
            self.addCleanup(self.dbsPatcher1.stop)
            self.addCleanup(self.dbsPatcher2.stop)

        if self.mockPhEDEx:
            self.phedexPatchers = []
            patchPhedexAt = ['WMCore.Services.PhEDEx.PhEDEx.PhEDEx', 'WMCore.WorkQueue.WorkQueue.PhEDEx',
                             'WMCore.Services.DBS.DBS3Reader.PhEDEx',
                             'WMComponent.PhEDExInjector.PhEDExInjectorPoller.PhEDEx']
            for module in patchPhedexAt:
                self.phedexPatchers.append(mock.patch(module, new=MockPhEDExApi))
                self.phedexPatchers[-1].start()
                self.addCleanup(self.phedexPatchers[-1].stop)

        if self.mockSiteDB:
            self.siteDBPatcher = mock.patch.object(SiteDBAPI, 'getJSON', new=mockGetJSON)
            self.inUseSiteDBApi = self.siteDBPatcher.start()
            self.addCleanup(self.siteDBPatcher.stop)

        return
