"""
_HartbeatAPI_

A simple object representing a file in WMBS.
"""

__revision__ = "$Id: HeartbeatAPI.py,v 1.1 2010/06/21 21:20:00 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import os
import logging

from WMCore.WMConnectionBase import WMConnectionBase

class HeartbeatAPI(WMConnectionBase):
    """
    Generic methods used by all of the WMBS classes.
    """
    def __init__(self, component, logger=None, dbi=None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WorkQueue as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMCore.Agent.Database", 
                                  logger = logger, dbi = dbi)
        
        self.component = component
        self.pid = os.getpid()
        
    def registerComponent(self):
        
        insertAction = self.daofactory(classname = "InsertComponent")
        insertAction.execute(self.component, self.pid,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
    def updateWorkerHeartbeat(self, workerName, state = None):
        
        existAction = self.daofactory(classname = "ExistWorker")
        componentID = existAction.execute(self.component, workerName, state,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        if not componentID:
            print componentID
            action = self.daofactory(classname = "InsertWorker")
            action.execute(self.component, workerName, state,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        else:
            action = self.daofactory(classname = "UpdateWorker")
            action.execute(componentID, workerName, state,
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
        
            
    
        