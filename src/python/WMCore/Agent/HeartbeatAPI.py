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
    def __init__(self, componentName, pollInterval=None, logger=None, dbi=None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attritbutes.  Create a DAO factory for WMCore.WorkQueue as well. Finally,
        check to see if a transaction object has been created.  If none exists,
        create one but leave the transaction closed.
        """
        WMConnectionBase.__init__(self, daoPackage = "WMCore.Agent.Database",
                                  logger = logger, dbi = dbi)

        self.insertComp = self.daofactory(classname = "InsertComponent")
        self.existWorker = self.daofactory(classname = "ExistWorker")
        self.insertWorker = self.daofactory(classname="InsertWorker")
        self.updateWorker = self.daofactory(classname="UpdateWorker")
        self.updateErrorWorker = self.daofactory(classname = "UpdateWorkerError")
        self.getHeartbeat = self.daofactory(classname = "GetHeartbeatInfo")
        self.getAllHeartbeat = self.daofactory(classname = "GetAllHeartbeatInfo")

        self.componentName = componentName
        self.pid = os.getpid()
        self.pollInterval = pollInterval

    def registerComponent(self):

        self.insertComp.execute(self.componentName, self.pid,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())

    def getComponentID(self, workerName):
        """Retrieve the component_id from the workers table"""

        componentID = self.existWorker.execute(self.componentName, workerName,
                                               conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
        return componentID

    def updateWorkerHeartbeat(self, workerName, state = "Start", pid = None, pollInt=None, timeSpent=None):
        """
        Update a worker's heartbeat. If it still doesn't exist, then add it
        with Start state.
        """
        componentID = self.getComponentID(workerName)

        if not componentID:
            pid = pid or self.pid
            pollInt = pollInt or self.pollInterval
            self.insertWorker.execute(self.componentName, workerName, state, pid, pollInt, cycleTime=0,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        else:
            self.updateWorker.execute(componentID, workerName, state, timeSpent,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())

    def updateWorkerCycle(self, workerName, timeSpent, results):
        """
        Update an already registered worker thread with the time it spent running
        the main algorithm call. It's state is unchanged.
        """
        componentID = self.getComponentID(workerName)

        self.updateWorker.execute(componentID, workerName, "Running", timeSpent, results,
                                  conn = self.getDBConn(),
                                  transaction = self.existingTransaction())

    def updateWorkerError(self, workerName, errorMessage):

        self.updateErrorWorker.execute(self.componentName, workerName, errorMessage,
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())

    def getHeartbeatInfo(self):

        results = self.getHeartbeat.execute(conn = self.getDBConn(),
                                            transaction = self.existingTransaction())

        return results

    def getAllHeartbeatInfo(self):

        results = self.getAllHeartbeat.execute(conn = self.getDBConn(),
                                               transaction = self.existingTransaction())

        return results
