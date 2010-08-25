#!/usr/bin/env python
#pylint: disable-msg=W6501, E1103
# W6501: pass information to logging using string arguments
# E1103: We attach argument to currentThread elsewhere
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.41 2010/06/09 19:13:31 sfoulkes Exp $"
__version__ = "$Revision: 1.41 $"

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

import threading
import time
import logging

class ChangeState(WMObject, WMConnectionBase):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config, couchDbName = None):
        WMObject.__init__(self, config)
        WMConnectionBase.__init__(self, "WMCore.WMBS")

        self.myThread = threading.currentThread()
        self.attachmentList = {}
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi

        self.database = None

        if couchDbName == None:
            couchDbName = getattr(self.config.JobStateMachine, "couchDBName",
                                   "Unknown")
            
        self.dbname = couchDbName

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        try:
            self.couchdb = None
            self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
            if self.dbname not in self.couchdb.listDatabases():
                self.createDatabase()

            self.database = self.couchdb.connectDatabase(couchDbName)
        except Exception, ex:
            logging.error("Error connecting to couch: %s" % str(ex))

        self.getCouchDAO = self.daoFactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daoFactory("Jobs.SetCouchID")
        return

    def propagate(self, jobs, newstate, oldstate):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        Take a list of job objects (dicts) and the desired state change.
        Return the jobs back, throw assertion error if the state change is not allowed
        and other exceptions as appropriate
        """
        if type(jobs) != list:
            jobs = [jobs]

        if len(jobs) == 0:
            return

        # 1. Is the state transition allowed?
        self.check(newstate, oldstate)
        # 2. Document the state transition
        try:
            self.recordInCouch(jobs, newstate, oldstate)
        except Exception, ex:
            logging.error("Error updating job in couch: %s" % str(ex))
            
        # 3. Make the state transition
        self.persist(jobs, newstate, oldstate)

        return jobs
    
    def check(self, newstate, oldstate):
        """
        check that the transition is allowed. return a tuple of the transition
        if it is allowed, throw up an exception if not.
        """
        newstate = newstate.lower()
        oldstate = oldstate.lower()
        
        # Check for wrong transitions
        transitions = Transitions()
        assert newstate in transitions[oldstate], \
                            "Illegal state transition requested"

    def recordInCouch(self, jobs, newstate, oldstate):
        """
        _recordInCouch_
        
        """
        if not self.database:
            return
        
        jobMap = {}
        jobIDsToCheck = []
        for job in jobs:
            jobMap[job["id"]] = job
            if job["couch_record"] == None:
                jobIDsToCheck.append(job["id"])

        couchIDs = self.getCouchDAO.execute(jobID = jobIDsToCheck,
                                            conn = self.getDBConn(),
                                            transaction = self.existingTransaction())

        for couchID in couchIDs:
            jobMap[couchID["jobid"]]["couch_record"] = couchID["couch_record"]

        timestamp = int(time.time())
        newJobCounter = 0
        baseUUID = makeUUID()
        couchRecordsToUpdate = []
        
        for jobID in jobMap:
            job = jobMap[jobID]
            couchDocID = job.get("couch_record", None)

            transitionDocument = {"jobid": job["id"],
                                  "oldstate": job["state"],
                                  "newstate": newstate,
                                  "timestamp": timestamp,
                                  "type": "state"}
            self.database.queue(transitionDocument)

            if couchDocID == None:
                jobDocument = {}
                jobDocument["_id"] = "%s_%s" % (baseUUID, newJobCounter)
                jobDocument["id"] = job["id"]
                jobDocument["input_files"] = job["input_files"]
                jobDocument["jobgroup"] = job["jobgroup"]
                jobDocument["mask"] = job["mask"]
                jobDocument["name"] = job["name"]

                couchRecordsToUpdate.append({"jobid": job["id"],
                                             "couchid": jobDocument["_id"]})                
                self.database.queue(jobDocument)
                newJobCounter += 1
            elif job.get("fwjr", None):
                fwjrDocument = {"jobid": job["id"],
                                "retrycount": job["retry_count"],
                                "fwjr": job["fwjr"].__to_json__(None),
                                "type": "fwjr"}
                self.database.queue(fwjrDocument)

        if len(couchRecordsToUpdate) > 0:
            self.setCouchDAO.execute(bulkList = couchRecordsToUpdate,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
            
        self.database.commit()
        return

    def createDatabase(self):
        ''' initializes a non-existant database'''
        database = self.couchdb.createDatabase(self.dbname)
        hashViewDoc = database.createDesignDoc('jobs')
        hashViewDoc["views"] = { }
     
        database.queue(hashViewDoc)
        database.commit()
        return database

    def persist(self, jobs, newstate, oldstate):
        """
        Write the state change to WMBS, via DAO
        """
        for job in jobs:
            job['state'] = newstate
            job['oldstate'] = oldstate
        dao = self.daoFactory(classname = "Jobs.ChangeState")
        dao.execute(jobs, conn = self.getDBConn(),
                    transaction = self.existingTransaction())
