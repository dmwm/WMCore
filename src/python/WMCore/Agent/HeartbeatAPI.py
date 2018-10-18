"""
_HartbeatAPI_

A simple object representing a file in WMBS.
"""

import os

from WMCore.Database.DBExceptionHandler import db_exception_handler
from WMCore.WMConnectionBase import WMConnectionBase


class HeartbeatAPI(WMConnectionBase):
    """
    Generic methods used by all of the WMBS classes.
    """

    def __init__(self, componentName, pollInterval=None, heartbeatTimeout=7200,
                 logger=None, dbi=None):
        """
        ___init___

        Initialize all the database connection attributes and the logging
        attributes.
        Every worker has a different instance of this class, thus they can have
        a different polling interval and a different heartbeat timeout.
        Finally, check to see if a transaction object has been created.
        If none exists, create one but leave the transaction closed.
        """
        self.componentName = componentName
        self.pollInterval = pollInterval
        self.heartbeatTimeout = heartbeatTimeout or 7200
        self.compId = os.getpid()

        WMConnectionBase.__init__(self, daoPackage="WMCore.Agent.Database",
                                  logger=logger, dbi=dbi)

        self.insertComp = self.daofactory(classname="InsertComponent")
        self.existWorker = self.daofactory(classname="ExistWorker")
        self.insertWorker = self.daofactory(classname="InsertWorker")
        self.updateWorker = self.daofactory(classname="UpdateWorker")
        self.updateErrorWorker = self.daofactory(classname="UpdateWorkerError")
        self.getHeartbeat = self.daofactory(classname="GetHeartbeatInfo")
        self.getAllHeartbeat = self.daofactory(classname="GetAllHeartbeatInfo")

    def registerComponent(self):
        """
        Deletes any leftover for a component with the same name and then
        inserts it again with a new PID into wm_components table
        """
        self.insertComp.execute(self.componentName, self.compId, self.heartbeatTimeout,
                                conn=self.getDBConn(), transaction=self.existingTransaction())

    def registerWorker(self, workerName, state="Start"):
        """
        Inserts a worker thread into the database, setting an initial state
        and a polling cycle.
        """
        self.insertWorker.execute(self.componentName, workerName, state,
                                  self.compId, self.pollInterval, cycleTime=0,
                                  conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

    def getComponentID(self, workerName):
        """Retrieve the component_id from the workers table"""

        componentID = self.existWorker.execute(self.componentName, workerName,
                                               conn=self.getDBConn(),
                                               transaction=self.existingTransaction())
        return componentID

    def updateWorkerHeartbeat(self, workerName, state):
        """
        Update a worker's heartbeat and its state
        """
        try:
            self.updateWorker.execute(workerName, state, conn=self.getDBConn(),
                                      transaction=self.existingTransaction())
        except Exception as ex:
            self.logger.warning("Heartbeat update failed! Wait for the next time...:\n%s", str(ex))

    @db_exception_handler
    def updateWorkerCycle(self, workerName, timeSpent, results):
        """
        Update a worker's heartbeat as well as the time spent on that
        cycle and any results returned.
        """
        self.updateWorker.execute(workerName, "Running", timeSpent, results,
                                  conn=self.getDBConn(),
                                  transaction=self.existingTransaction())

    @db_exception_handler
    def updateWorkerError(self, workerName, errorMessage):

        self.updateErrorWorker.execute(self.componentName, workerName, errorMessage,
                                       conn=self.getDBConn(),
                                       transaction=self.existingTransaction())

    def getHeartbeatInfo(self):

        results = self.getHeartbeat.execute(self.componentName, conn=self.getDBConn(),
                                            transaction=self.existingTransaction())

        return results

    def getAllHeartbeatInfo(self):

        results = self.getAllHeartbeat.execute(conn=self.getDBConn(),
                                               transaction=self.existingTransaction())

        return results
