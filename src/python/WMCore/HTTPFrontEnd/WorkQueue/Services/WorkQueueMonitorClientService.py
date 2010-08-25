import os
import time
import logging # import WMCore.WMLogging
from WMCore.HTTPFrontEnd.WorkQueue.Services.ServiceInterface import ServiceInterface

class WorkQueueMonitorClientService(ServiceInterface):
    _myClass = "short cut to the class name for logging purposes"
    
    def register(self):
        self.model.addMethod("GET", "teststatus", self.testStatus)
        self.model.addMethod("GET", "testhtml", self.testHtml)
        
    def testStatus(self):    
        return self.model.templatepage("StatusTable") 
        
    def testHtml(self):    
        return self.model.templatepage("HTMLTest")