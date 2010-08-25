"""
_HartbeatAPI_

A simple object representing a file in WMBS.
"""




import threading
import os
import logging

from WMCore.WMConnectionBase import WMConnectionBase

class HeartbeatAPI(WMConnectionBase):
    """
    Generic methods used by all of the WMBS classes.
    """
    def __init__(self, componentName, logger=None, dbi=None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WorkQueue as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMCore.Agent.Database", 
                                  logger = logger, dbi = dbi)
        
        self.componentName = componentName
        self.pid = os.getpid()
        
    def registerComponent(self):
        
        insertAction = self.daofactory(classname = "InsertComponent")
        insertAction.execute(self.componentName, self.pid,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
    def updateWorkerHeartbeat(self, workerName, state = "Start", pid = None):
        
        existAction = self.daofactory(classname = "ExistWorker")
        componentID = existAction.execute(self.componentName, workerName, 
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        if not componentID:
            action = self.daofactory(classname = "InsertWorker")
            action.execute(self.componentName, workerName, state, pid,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "UpdateWorker")
            action.execute(componentID, workerName, state, pid,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
    
    def updateWorkerError(self, workerName, errorMessage):
        
        action = self.daofactory(classname = "UpdateWorkerError")
        action.execute(self.componentName, workerName, errorMessage,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
            
    def getHeartbeatInfo(self):
        
        heartbeatInfo = self.daofactory(classname = "GetHeartbeatInfo")
        results = heartbeatInfo.execute(conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
        return results
        
    
    def getAllHeartbeatInfo(self):
        
        heartbeatInfo = self.daofactory(classname = "GetAllHeartbeatInfo")
        results = heartbeatInfo.execute(conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
        return results
        
            
    
        
