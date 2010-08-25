"""
WMCore/HTTPFrontEnd/WorkQueue/Services/WorkQueueMonitorService.py

REST interface to WorkQueue monitoring capabilities, output in
    DAS-compatible format.
Provides monitoring of WorkQueue elements, using DAO, DAO classes
    residing in WorkQueue/Database/

requirements:
https://twiki.cern.ch/twiki/bin/viewauth/CMS/WMCoreDiscussT1Rollout
Provide monitoring information for the request as it propagates down to the agents

installation:
https://twiki.cern.ch/twiki/bin/view/CMS/WorkQueueInstallation

writing unittests / testing details, hints:
https://twiki.cern.ch/twiki/bin/view/CMS/RESTModelUnitTest
WMCore/Services/WorkQueue/WorkQueue.py
WMCore_t/Webtools_t/ and WMCore_t/Services_t/WorkQueue_t/WorkQueue_t.py

TODO:
-more monitoring requirements (e.g. statistics, etc) - not clear now
"""


__revision__ = "$Id: WorkQueueMonitorService.py,v 1.10 2010/03/24 19:41:47 sryu Exp $"
__version__ = "$Revision"


import os
import time
import logging # import WMCore.WMLogging
from WMCore.WorkQueue.WorkQueue import WorkQueue
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface
from WMCore.DAOFactory import DAOFactory
from WMCore.WorkQueue.Database import States


class WorkQueueMonitorService(ServiceInterface):
    _myClass = "short cut to the class name for logging purposes"
    
    def register(self):
        self._myClass = self.__class__.__name__ 
        
        
        #self._testDbReadiness()

        self.model.addMethod("GET", "test", self.testMethod)

        # from WorkQueueService
        self.wq = WorkQueue(logger = self.model, dbi = self.model.dbi, **self.model.config.queueParams)
        
        self.model.addMethod("GET", "status", self.wq.status,
                             args = ["status", "before", "after", "elementIDs", "dictKey"])
        
        # DAO stuff
        # RESTModel.addDAO() see COMP/T0/src/python/T0/DAS/Tier0RESTModel.py
        # (within WMCore no addDAO() example except for WebTools_t/DummyRESTModel.py ...)
        
        self.model.daofactory = DAOFactory(package = "WMCore.WorkQueue.Database",
                                           logger = self.model,
                                           dbinterface = self.model.dbi)
        # WorkQueue.status signature:
        # status(self, status = None, before = None, after = None, elementIDs = None, dictKey = None)
        
        self.model.addDAO("GET",  "elements", "Monitor.Elements")
        self.model.addDAO("GET",  "sites", "Monitor.Sites")
        self.model.addDAO("GET",  "data", "Monitor.Data")
        self.model.addDAO("GET",  "datasitemap", "Monitor.DataSiteMap")
        self.model.addDAO("POST", "elementsbystate", "Monitor.ElementsByState",
                           args = ["status"], validation = [self.validateState])
        self.model.addDAO("POST", "elementsbyid", "Monitor.ElementsById",
                           args = ["id"], validation = [self.validateId])
        
        logging.info("%s initialised." % self._myClass)        
        


    def validateState(self, inpt):
        """Validate inpt argument state - only element states as defined in
           States (WMCore.WorkQueue.Database) are accepted (i.e. only states
           designated by respective string names, not by integer indices).
        """
        state = inpt["status"]
        try:
            try:
                int(state)
            except:
                pass
            else:
                raise ValueError # is integer - fail
            States[state]
        except (ValueError, KeyError, TypeError):
            m = "Incorrect input - unknown WorkQueue element state '%s'" % state
            raise AssertionError, m
        else:
            return inpt
        
        
    
    def validateId(self, inpt):
        """Validate inpt argument id - only positive integers allowed."""
        id = inpt["id"]
        try:
            if int(id) < 0:
                raise ValueError
        except (ValueError, TypeError):
            m = "Incorrect input - id must be positive integer ('%s')" % id            
            raise AssertionError, m
        else:
            return inpt
                
        

    # -----------------------------------------------------------------------
    # dummy tests, database connection testing / experiments stuff
    # to be removed later (2010-02-05)  
    
    
    def _testDbReadiness(self):
        """town table used by a WebTools tutorial example"""
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

