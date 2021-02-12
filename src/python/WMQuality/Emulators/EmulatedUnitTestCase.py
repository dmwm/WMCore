#! /usr/bin/env python
"""
Unit testing base class that turns on emulators
"""

from __future__ import (division, print_function)

import unittest

import mock

from WMQuality.Emulators.CRICClient.MockCRICApi import MockCRICApi
from WMQuality.Emulators.Cache.MockMemoryCacheStruct import MockMemoryCacheStruct
from WMQuality.Emulators.DBSClient.MockDbsApi import MockDbsApi
from WMQuality.Emulators.DashboardApMon.MockApMon import MockApMon
from WMQuality.Emulators.LogDB.MockLogDB import MockLogDB
from WMQuality.Emulators.PhEDExClient.MockPhEDExApi import MockPhEDExApi
from WMQuality.Emulators.PyCondorAPI.MockPyCondorAPI import MockPyCondorAPI
from WMQuality.Emulators.ReqMgrAux.MockReqMgrAux import MockReqMgrAux
from WMQuality.Emulators.RucioClient.MockRucioApi import MockRucioApi


class EmulatedUnitTestCase(unittest.TestCase):
    """
    Class that can be imported to switch to 'mock'ed versions of
    services.
    """

    def __init__(self, methodName='runTest', mockDBS=True, mockPhEDEx=True,
                 mockReqMgrAux=True, mockLogDB=True,
                 mockApMon=True, mockMemoryCache=True, mockPyCondor=True,
                 mockCRIC=True, mockRucio=True):
        self.mockDBS = mockDBS
        self.mockPhEDEx = mockPhEDEx
        self.mockReqMgrAux = mockReqMgrAux
        self.mockLogDB = mockLogDB
        self.mockApMon = mockApMon
        self.mockMemoryCache = mockMemoryCache
        self.mockPyCondor = mockPyCondor
        self.mockCRIC = mockCRIC
        self.mockRucio = mockRucio
        super(EmulatedUnitTestCase, self).__init__(methodName)

    def setUp(self):
        """
        Start the various mocked versions and add cleanups in case of exceptions
        Note: patch has to be applied to where the object is loaded/imported, not
        to its origin location.

        TODO: parameters to turn off emulators individually
        """

        if self.mockDBS:
            self.dbsPatchers = []
            patchDBSAt = ["dbs.apis.dbsClient.DbsApi",
                          "WMCore.Services.DBS.DBS3Reader.DbsApi"]
            for module in patchDBSAt:
                self.dbsPatchers.append(mock.patch(module, new=MockDbsApi))
                self.dbsPatchers[-1].start()
                self.addCleanup(self.dbsPatchers[-1].stop)

        if self.mockPhEDEx:
            self.phedexPatchers = []
            patchPhedexAt = ['WMCore.Services.PhEDEx.PhEDEx.PhEDEx',
                             'WMCore.WorkQueue.WorkQueue.PhEDEx',
                             'WMComponent.PhEDExInjector.PhEDExInjectorPoller.PhEDEx']
            for module in patchPhedexAt:
                self.phedexPatchers.append(mock.patch(module, new=MockPhEDExApi))
                self.phedexPatchers[-1].start()
                self.addCleanup(self.phedexPatchers[-1].stop)

        if self.mockRucio:
            self.rucioPatchers = []
            patchRucioAt = ['WMCore.WorkQueue.WorkQueue.Rucio',
                            'WMCore.WorkQueue.WorkQueueReqMgrInterface.Rucio',
                            'WMCore.WorkQueue.Policy.Start.StartPolicyInterface.Rucio']
            for module in patchRucioAt:
                self.rucioPatchers.append(mock.patch(module, new=MockRucioApi))
                self.rucioPatchers[-1].start()
                self.addCleanup(self.rucioPatchers[-1].stop)

        if self.mockReqMgrAux:
            self.reqMgrAuxPatchers = []
            patchReqMgrAuxAt = ['WMCore.Services.ReqMgrAux.ReqMgrAux.ReqMgrAux',
                                'WMComponent.JobSubmitter.JobSubmitterPoller.ReqMgrAux',
                                'WMComponent.ErrorHandler.ErrorHandlerPoller.ReqMgrAux']
            for module in patchReqMgrAuxAt:
                self.reqMgrAuxPatchers.append(mock.patch(module, new=MockReqMgrAux))
                self.reqMgrAuxPatchers[-1].start()
                self.addCleanup(self.reqMgrAuxPatchers[-1].stop)

        if self.mockLogDB:
            self.logDBPatcher = mock.patch('WMCore.Services.LogDB.LogDB.LogDB',
                                           new=MockLogDB)
            self.inUseLogDB = self.logDBPatcher.start()
            self.addCleanup(self.logDBPatcher.stop)

        if self.mockApMon:
            self.apMonPatchers = []
            patchApMonAt = ['WMCore.Services.Dashboard.apmon.ApMon',
                            'WMCore.Services.Dashboard.DashboardAPI.apmon.ApMon']
            for module in patchApMonAt:
                self.apMonPatchers.append(mock.patch(module, new=MockApMon))
                self.apMonPatchers[-1].start()
                self.addCleanup(self.apMonPatchers[-1].stop)

        if self.mockMemoryCache:
            self.memoryCachePatcher = mock.patch('WMCore.Cache.GenericDataCache.MemoryCacheStruct',
                                                 new=MockMemoryCacheStruct)
            self.inUseMemoryCache = self.memoryCachePatcher.start()
            self.addCleanup(self.memoryCachePatcher.stop)

        if self.mockPyCondor:
            self.condorPatcher = mock.patch('WMCore.Services.PyCondor.PyCondorAPI.PyCondorAPI',
                                            new=MockPyCondorAPI)
            self.condorPatcher.start()
            self.addCleanup(self.condorPatcher.stop)

        if self.mockCRIC:
            self.cricPatchers = []
            patchCRICAt = ['WMCore.ReqMgr.Tools.cms.CRIC',
                           'WMCore.WorkQueue.WorkQueue.CRIC',
                           'WMCore.WorkQueue.WorkQueueUtils.CRIC',
                           'WMCore.WorkQueue.Policy.Start.StartPolicyInterface.CRIC']
            for module in patchCRICAt:
                self.cricPatchers.append(mock.patch(module, new=MockCRICApi))
                self.cricPatchers[-1].start()
                self.addCleanup(self.cricPatchers[-1].stop)

        return
