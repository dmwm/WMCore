"""
WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueMonitorService.py

REST interface to WorkQueue monitoring capabilities.

requirements:
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMCoreDiscussT1Rollout
Provide monitoring information for the request as it propagates down to the agents

installation:
https://twiki.cern.ch/twiki/bin/view/CMS/WorkQueueInstallation

writing unittests / testing details:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest



"""




__revision__ = "$Id: WorkQueueMonitorService.py,v 1.3 2010/02/03 17:20:49 maxa Exp $"
__version__ = "$Revision: 1.3 $"



import os
import time
import logging # import WMCore.WMLogging
from WMCore.Wrappers import JsonWrapper
from WMCore.WorkQueue.WorkQueue import globalQueue
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface
from WMCore.DAOFactory import DAOFactory


class WorkQueueMonitorService(ServiceInterface):
    _myClass = "short cut to the class name for logging purposes"
    
    def register(self):
        self._myClass = self.__class__.__name__ 
        
        self._testDbReadiness()

        self.model.addMethod("GET", "test", self.testMethod)
        self.model.addMethod("GET", "testDb", self.testDb)

        # from Seangchan (WorkQueueService)
        self.wq = globalQueue(logger = self.model, dbi = self.model.dbi, **self.model.config.queueParams)
        self.model.addMethod('POST', 'status', self.wq.status, args=["status", "before", "after", 
                                        "elementIDs", "subs", "dictKey"])
        
        # DAO stuff
        # RESTModel.addDAO() usage: COMP/T0/src/python/T0/DAS/Tier0RESTModel.py
        # (within WMCore no addDAO example except for WebTools_t/DummyRESTModel.py ...)
        
        # AttributeError: 'WorkQueueRESTModel' object has no attribute 'daofactory'
        self.model.daofactory = DAOFactory(package = "WMCore.WorkQueue.Database",
                                           logger = self.model,
                                           dbinterface = self.model.dbi)
        
        # WorkQueue.status signature:
        # status(self, status = None, before = None, after = None, elementIDs = None, dictKey = None)
        
        # DAO elements
        # DAO elements by status
        # TODO
        # DAO elements ... which else possibilities to add?
        
        self.model.addDAO("GET",  "elements", "Monitor.Elements")
        self.model.addDAO("POST", "elementsbystatus", "ElementsByStatus", args = ["status"])
        
        logging.info("%s initialised." % self._myClass)        
        

    
    def _testDbReadiness(self):
        logging.debug("%s doing database readiness test." % self._myClass)
        try:
            sql = "create table towns (name varchar(20), country varchar(20))"
            self.model.dbi.processData(sql)
        except Exception, ex:
            logging.error("database error, reason: '%s'" % ex)
            
        try:
            sql = "insert into towns (name, country) values ('SomeTown', 'SomeCountry')"
            self.model.dbi.processData(sql)
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
            result = self.model.dbi.processData("select * from towns")
            return self.model.formatDict(result)
        except Exception, ex:
            return "database test failed, reason: '%s'" % ex
        
        