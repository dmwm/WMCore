#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.29 2009/10/12 19:23:27 sfoulkes Exp $"
__version__ = "$Revision: 1.29 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

import base64
import urllib
from sets import Set
import threading
import time

class ChangeState(WMObject, WMConnectionBase):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config={}, couchDbName = 'jsm_job_history'):
        WMObject.__init__(self, config)
        WMConnectionBase.__init__(self, "WMCore.WMBS")

        self.myThread = threading.currentThread()
        self.attachmentList = {}
        self.logger = self.myThread.logger
        self.dialect = self.myThread.dialect
        self.dbi = self.myThread.dbi
        self.dbname = couchDbName
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
        self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
        if self.dbname not in self.couchdb.listDatabases():
            self.createDatabase()

        self.database = self.couchdb.connectDatabase(couchDbName)

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
        self.recordInCouch(jobs, newstate, oldstate)
        # 3. Make the state transition
        self.persist(jobs, newstate, oldstate)

        return jobs
    
    def addAttachment(self,name,jobid,url):
        """
            addAttachment(name, jobid, url)
        enqueues an attachment to be stuck onto the couchrecord at the
        next recordInCouch() call
        """
        if (not jobid in self.attachmentList):
            self.attachmentList[jobid] = {}
        self.attachmentList[jobid][name] = url
        return
    
    def getAttachment(self, couchID, name):
        """
            getAttachment(couchID, name)
        returns an attachment with the given name in the couch record
        identified by couchID
        """
        return self.database.getAttachment(couchID, name)
    
    
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
        
        Record a state transition in couch.  Each job will have it's own
        document and the transitions will be stored as a list of dicts with each
        dict having the following keys:
          oldstate
          newstate
          timestamp
        """
        transDict = {"oldstate": oldstate, "newstate": newstate,
                     "timestamp": int(time.time())}

        getCouchDAO = self.daoFactory("Jobs.GetCouchID")
        setCouchDAO = self.daoFactory("Jobs.SetCouchID")

        for job in jobs:
            doc = None
            couchRecord = None
            
            if job["couch_record"] == None:
                couchRecord = getCouchDAO.execute(jobID = job["id"],
                                                  conn = self.getDBConn(),
                                                  transaction = self.existingTransaction())

                if couchRecord == None:
                    doc = job
                    doc["_id"] = makeUUID()
                    job["couch_record"] = doc["_id"]
                    doc["state_changes"] = []
                    doc["fwkjrs"] = []

                    setCouchDAO.execute(jobID = job["id"], couchID = doc["_id"],
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
                    couchRecord = doc["_id"]
            else:
                couchRecord = job["couch_record"]

            if doc == None:
                doc = self.database.document(couchRecord)
                doc["retry_count"] = job["retry_count"]
                doc["outcome"] = job["outcome"]
                doc["state"] = job["state"]

            doc["state_changes"].append(transDict)

            if job.has_key("fwjr"):
                doc["fwkjrs"].append(job["fwjr"])
                del job["fwjr"]

            self.database.queue(doc)

        docsCommitted = self.database.commit()
        assert len(jobs) == len(docsCommitted), \
               "Got less than I was expecting from CouchDB: \n %s" %\
                        (goodresult,)
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
