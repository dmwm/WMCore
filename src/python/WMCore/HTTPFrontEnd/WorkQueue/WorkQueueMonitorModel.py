"""
WMCore/HTTPFrontEnd/WorkQueue/WorkQueueMonitoringModel.py

REST interface to WorkQueue monitoring capabilities.

requirements:
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMCoreDiscussT1Rollout
Provide monitoring information for the request as it propagates down to the agents

installation:
https://twiki.cern.ch/twiki/bin/view/CMS/WorkQueueInstallation

writing unittests / testing details:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest

"""



__revision__ = "$Id: WorkQueueMonitorModel.py,v 1.2 2010/01/28 15:46:46 maxa Exp $"
__version__ = "$Revision: 1.2 $"



import os
import time
import logging # import WMCore.WMLogging
from WMCore.WebTools.RESTModel import RESTModel
from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue



class WorkQueueMonitorModel(RESTModel):
    _myClass = "short cut to the class name for logging purposes"
    def __init__(self, config = {}):
        self._myClass = self.__class__.__name__ 
        RESTModel.__init__(self, config)
        
        self._testDbReadiness()
        
        # create an instance of WorkQueue
        # this one fails - AttributeError: 'ConfigSection' object has no attribute 'queueParams'
        # self.wq = globalQueue(logger = self, dbi = self.dbi, **config.queueParams)

        # taken from test/python/WMCore_t/WorkQueue_t/WorkQueue_t.py
        self.globalQueue = globalQueue(CacheDir = 'global',
                                       NegotiationTimeout = 0,
                                       QueueURL = 'global.example.com')

        # 
        #self.globalQueue = getGlobalQueue(dbi = self.testInit.getDBInterface(),
        #                                  CacheDir = 'global',
        #                                  NegotiationTimeout = 0,
        #                                  QueueURL = self.config.getServerUrl())    



        self.addMethod("GET", "test", self.testMethod)
        self.addMethod("GET", "testDb", self.testDb)
        # TODO - args should not be listed this way ...
        self.addMethod("POST", "status", self.status,
                       args=["status", "before", "after", "elementIDs", "subs", "dictKey"])
        
        # must be DASFormatter   
        # later using DAO (should use self.addDAO() ...
            
        logging.info("%s initialised." % self._myClass)
        

    
    def _testDbReadiness(self):
        logging.debug("%s doing database readiness test." % self._myClass)
        try:
            sql = "create table towns (name varchar(20), country varchar(20))"
            self.dbi.processData(sql)
        except Exception, ex:
            logging.error("database error, reason: '%s'" % ex)
            
        try:
            sql = "insert into towns (name, country) values ('SomeTown', 'SomeCountry')"
            self.dbi.processData(sql)
        except Exception, ex:
            logging.error("database error, reason: '%s'" % ex)
            
        logging.debug("%s database readiness test finished." % self._myClass)



    def testMethod(self):
        """testMethot - returns simple data - current time"""
        format = "%d %b %Y %H:%M:%S %Z"
        r = "date/time: %s" % time.strftime(format, time.localtime())
        return r



    def testDb(self):        
        try:            
            result = self.dbi.processData("select * from towns")
            return self.formatDict(result)
        except Exception, ex:
            return "database test failed, reason: '%s'" % ex
        
        
        
    def status(self, status = None, before = None, after = None, elementIDs=None, 
               dictKey = None):
        """Monitoring status of the WorkQueue elements"""
        
        if elementIDs != None:
            elementIDs = JsonWrapper.loads(elementIDs)
        
        if before != None:
            before = int(before)
        if after != None:
            after = int(after)
        
        result = self.globalQueue.status(status, before, after, elementIDs,
                                         dictKey)
        
        return result        
                        