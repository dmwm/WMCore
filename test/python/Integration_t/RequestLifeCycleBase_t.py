#!/usr/bin/env python
"""Base class for integration test for request lifecycle"""

from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Cache.WMConfigCache import ConfigCache
from WMCore.WMBase import getTestBase

import nose
from nose.plugins.attrib import attr
import time
import os
import imp

class RequestLifeCycleBase_t():

    request = None
    request_name = None
    workqueue = None
    endpoint = os.environ.get('REQMGRBASEURL', 'https://localhost:8443')
    reqmgr = RequestManager({'endpoint' : endpoint + '/reqmgr/reqMgr'})
    team = 'TestTeam'

    def setUp(self):
        pass

    def _configCacheId(self, label):
        """Return config cache id for given config label"""
        key, cert = self.__class__.reqmgr['requests'].getKeyCert()
        configCache = ConfigCache(self.__class__.endpoint + '/couchdb', 'reqmgr_config_cache', ckey = key, cert = cert)
        try:
            configCacheId = configCache.getIDFromLabel(label)
        except:
            configCacheId = None
        if configCacheId:
            return configCacheId
        # The following will fail if FWCore.ParameterSet not in PYTHONPATH
        from PSetTweaks.WMTweak import makeTweak
        configCache.createUserGroup('test', 'test')
        configDir = os.path.join(getTestBase(), '..', '..', 'test', 'data', 'configs')
        configCache.addConfig(os.path.join(configDir, label + '.py'))
        configCache.setLabel(label)
        configCache.setDescription(label)
        modPath = imp.find_module(label, [configDir])
        loadedConfig = imp.load_module(label, modPath[0], modPath[1], modPath[2])
        configCache.setPSetTweaks(makeTweak(loadedConfig.process).jsondictionary())
        configCache.save()
        return configCache.getIDFromLabel(label)

    def _convertLabelsToId(self, config):
        fields = ['ProcConfigCacheID', 'Skim1ConfigCacheID',
                  'StepOneConfigCacheID', 'ConfigCacheID']
        for field in fields:
            if config.get(field):
                config[field] = self._configCacheId(config[field])
        for field in ['Task1', 'Task2', 'Task3', 'Task4']:
            if config.get(field):
                config[field] = self._convertLabelsToId(config[field])
        return config

    @attr("lifecycle")
    def test05InjectConfigs(self):
        """Inject configs to cache"""
        self.__class__.requestParams = self._convertLabelsToId(self.__class__.requestParams)

    @attr("lifecycle")
    def test10InjectRequest(self):
        """Can inject a request"""
        self.__class__.requestParams.setdefault('RequestString', self.__class__.__name__)
        tries = 0
        while True:
            try:
                self.__class__.request = self.__class__.reqmgr.makeRequest(**self.__class__.requestParams)['WMCore.RequestManager.DataStructs.Request.Request']
                self.__class__.request_name = self.__class__.request['RequestName']
                break
            except:
                tries += 1
                if tries > 3:
                    raise
        self.assertTrue(self.__class__.request)
        self.assertTrue(self.__class__.request_name)
        print "Injected request %s" % self.__class__.request_name
        self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
        self.assertEqual(self.__class__.request['RequestStatus'], 'new')

    @attr("lifecycle")
    def test20ApproveRequest(self):
        """Approve request"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        self.__class__.reqmgr.reportRequestStatus(self.__class__.request_name, 'assignment-approved')
        self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
        self.assertEqual(self.__class__.request['RequestStatus'], 'assignment-approved')#

    @attr("lifecycle")
    def test30AssignRequest(self):
        """Assign request"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        self.__class__.reqmgr.assign(self.__class__.request_name, self.__class__.team, "Testing", "v1",
                           MergedLFNBase='/store/temp', UnmergedLFNBase='/store/temp')
        self.__class__.request = self.reqmgr.getRequest(self.__class__.request_name)
        self.assertEqual(self.__class__.request['RequestStatus'], 'assigned')

    @attr("lifecycle")
    def test40WorkQueueAcquires(self):
        """WorkQueue picks up request"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            workqueue = self.reqmgr.getWorkQueue(request = self.__class__.request_name)
            if workqueue:
                self.__class__.workqueue = WorkQueue(workqueue[0])
                self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
                self.assertTrue(self.__class__.request['RequestStatus'] in ('acquired', 'running'))
                request = [x for x in self.__class__.workqueue.getJobStatusByRequest() if \
                    x['request_name'] == self.__class__.request_name]
                if [x for x in request if x['status'] in ('Available', 'Negotiating', 'Acquired', 'Running')]:
                    break
            if start + (60 * 20) < time.time():
                raise RuntimeError, 'timeout waiting for workqueue to acquire'
            time.sleep(15)

    @attr("lifecycle")
    def test50AgentAcquires(self):
        """Elements acquired by agent"""
        # skip if request already running
        self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
        if not self.__class__.request_name or self.__class__.request['RequestStatus'] == 'running':
            raise nose.SkipTest
        start = time.time()
        while True:
            request = [x for x in self.__class__.workqueue.getJobStatusByRequest() if \
                        x['request_name'] == self.__class__.request_name]
            if [x for x in request if x['status'] in ('Acquired', 'Running')]:
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError, 'timeout waiting for agent to acquire'
            time.sleep(15)
        self.assertTrue([x for x in request if x['status'] in ('Acquired', 'Running')])

    @attr("lifecycle")
    def test60RequestRunning(self):
        """Request running"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            request = [x for x in self.__class__.workqueue.getJobStatusByRequest() if \
                    x['request_name'] == self.__class__.request_name]
            childQueue = [x for x in self.__class__.workqueue.getChildQueuesByRequest() if \
                    x['request_name'] == self.__class__.request_name]
            if request and 'Running' in [x['status'] for x in request]:
                self.assertTrue(childQueue, "Running but can't get child queue")
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError, 'timeout waiting for request to run'
            time.sleep(15)

    @attr("lifecycle")
    def test70WorkQueueFinished(self):
        """Request completed in workqueue"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            request = [x for x in self.__class__.workqueue.getJobStatusByRequest() if \
                    x['request_name'] == self.__class__.request_name]
            # request deleted from wq shortly after finishing, so may not appear here
            if not request or request == [x for x in request if x['status'] in ('Done', 'Failed', 'Canceled')]:
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError, 'timeout waiting for request to finish'
            time.sleep(15)

    @attr("lifecycle")
    def test80RequestFinished(self):
        """Request completed"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        start = time.time()
        while True:
            self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
            if self.__class__.request['RequestStatus'] in ('completed', 'failed',
                                                           'aborted'):
                break
            if start + (60 * 20) < time.time():
                raise RuntimeError, 'timeout waiting for request to finish'
            time.sleep(15)

    @attr("lifecycle")
    def test90RequestCloseOut(self):
        """Closeout request"""
        if not self.__class__.request_name:
            raise nose.SkipTest
        self.reqmgr.reportRequestStatus(self.__class__.request_name, "closed-out")
        self.__class__.request = self.__class__.reqmgr.getRequest(self.__class__.request_name)
        self.assertEqual('closed-out', self.__class__.request['RequestStatus'])
