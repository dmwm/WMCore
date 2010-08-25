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


__revision__ = "$Id: WorkQueueMonitorModel.py,v 1.1 2010/01/26 15:14:41 maxa Exp $"
__version__ = "$Revision: 1.1 $"



import os
import time
import logging # import WMCore.WMLogging
from WMCore.WebTools.RESTModel import RESTModel



class WorkQueueMonitorModel(RESTModel):
    _myClass = "short cut to the class name for logging purposes"
    def __init__(self, config = {}):
        self._myClass = self.__class__.__name__ 
        RESTModel.__init__(self, config)
        
        self._testDbReadiness()
        
        self.addMethod("GET", "test", self.testMethod)
        self.addMethod("GET", "testDb", self.testDb)        
            
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
        return "date/time: %s" % time.strftime(format, time.localtime())



    def testDb(self):        
        try:            
            result = self.dbi.processData("select * from towns")
            return self.formatDict(result)
        except Exception, ex:
            return "database test failed, reason: '%s'" % ex
                
                