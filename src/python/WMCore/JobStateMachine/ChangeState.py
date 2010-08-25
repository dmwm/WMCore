#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.38 2010/04/12 20:31:39 sfoulkes Exp $"
__version__ = "$Revision: 1.38 $"

from WMCore.Database.Transaction import Transaction
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

import base64
import urllib
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
        results = None
        
        try:
            results = self.database.getAttachment(couchID, name)
        except Exception, ex:
            logging.error("Error retrieving attachment: %s" % str(ex))
            
        return results 
    
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

        jobIDNoCouch = []
        for job in jobs:
            job["state"] = newstate
            if job["couch_record"] == None:
                jobIDNoCouch.append(job['id'])
        couchRecordList = getCouchDAO.execute(jobID = jobIDNoCouch, conn = self.getDBConn(),
                                              transaction = self.existingTransaction())
        for job in jobs:
            for record in couchRecordList:
                if job['id'] == record['jobid']:
                    job["couch_record"] = record['couch_record']
                    break
        uuID          = None
        newJobCounter = 0

        couchRecordsToUpdate = []
                
        for job in jobs:
            doc = None
            couchRecord = job.get('couch_record', None)
            
            if job["couch_record"] == None:
                doc = job
                if not uuID:
                    uuID = makeUUID()
                doc["_id"] = '%s_%i' %(uuID, newJobCounter)
                newJobCounter += 1
                job["couch_record"] = doc["_id"]
                doc["state_changes"] = []
                doc["fwkjrs"] = []
                couchRecordsToUpdate.append({'jobid': job['id'], 'couchid': doc['_id']})
                couchRecord = doc["_id"]

            else:
                couchRecord = job["couch_record"]

            if doc == None:
                doc = self.database.document(couchRecord)
                doc["retry_count"] = job["retry_count"]
                doc["outcome"] = job["outcome"]
                doc["state"] = job["state"]

            doc["state_changes"].append(transDict)

            if job["fwjr"] != None:
                doc["fwkjrs"].append(job["fwjr"])
                del job["fwjr"]

            self.database.queue(doc)

        if len(couchRecordsToUpdate) > 0:
            setCouchDAO.execute(bulkList = couchRecordsToUpdate, conn = self.getDBConn(),
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
