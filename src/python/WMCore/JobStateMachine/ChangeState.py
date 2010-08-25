#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

__revision__ = "$Id: ChangeState.py,v 1.49 2010/06/21 19:46:03 sfoulkes Exp $"
__version__ = "$Revision: 1.49 $"

from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

import threading
import time
import logging

stateTransitionsByJobID = {"map": \
"""
function(doc) {
  if (doc['type'] == 'state') {
    emit(doc['jobid'], {'oldstate': doc['oldstate'],
                        'newstate': doc['newstate'],
                        'timestamp': doc['timestamp']});
    }
  }
"""}

fwjrsByJobID = {"map": \
"""
function(doc) {
  if (doc['type'] == 'fwjr') {
    emit(doc['jobid'], {'_id': doc['_id']});
    }
  }
"""}  

jobsByJobID = {"map": \
"""
function(doc) {
  if (doc['type'] == 'job') {
    emit(doc['jobid'], {'_id': doc['_id']});
    }
  }
"""}  

class ChangeState(WMObject, WMConnectionBase):
    """
    Propagate the state of a job through the JSM.
    """
    def __init__(self, config, couchDbName = None):
        WMObject.__init__(self, config)
        WMConnectionBase.__init__(self, "WMCore.WMBS")

        if couchDbName == None:
            self.dbname = getattr(self.config.JobStateMachine, "couchDBName",
                                  "Unknown")
        else:
            self.dbname = couchDbName

        try:
            self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
            if self.dbname not in self.couchdb.listDatabases():
                self.createDatabase()

            self.database = self.couchdb.connectDatabase(self.dbname)
        except Exception, ex:
            logging.error("Error connecting to couch: %s" % str(ex))

        self.getCouchDAO = self.daofactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daofactory("Jobs.SetCouchID")
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
        return
    
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

        Record relevant job information in couch.  This will always record the
        state change information as a seperate document.  If the job does not
        yet exist in couch it will be saved as a seperate document.  If the job
        has a FWJR attached that will be saved as a seperate document.
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
        
        for jobID in jobMap.keys():
            job = jobMap[jobID]
            couchDocID = job.get("couch_record", None)

            if newstate == "new":
                oldstate = "none"
            else:
                oldstate = job["state"]
                
            transitionDocument = {"jobid": job["id"],
                                  "oldstate": oldstate,
                                  "newstate": newstate,
                                  "timestamp": timestamp,
                                  "type": "state"}
            self.database.queue(transitionDocument)

            if couchDocID == None:
                jobDocument = {}
                jobDocument["_id"] = job["id"]
                job["couch_record"] = jobDocument["_id"]
                jobDocument["jobid"] = job["id"]
                jobDocument["workflow"] = job["workflow"]
                jobDocument["task"] = job["task"]
                jobDocument["owner"] = job["owner"]
                jobDocument["inputfiles"] = []
                for inputFile in job["input_files"]:
                    docInputFile = {"lfn": inputFile["lfn"],
                                    "firstevent": inputFile["first_event"],
                                    "lastevent": inputFile["last_event"],
                                    "id": inputFile["id"],
                                    "size": inputFile["size"],
                                    "events": inputFile["events"],
                                    "merged": inputFile["merged"],
                                    "locations": [],
                                    "runs": [],
                                    "parents": []}

                    for location in inputFile["locations"]:
                        docInputFile["locations"].append(location)

                    for parent in inputFile["parents"]:
                        docInputFile["parents"].append({"lfn": parent["lfn"]})

                    jobDocument["inputfiles"].append(docInputFile)
                        
                jobDocument["jobgroup"] = job["jobgroup"]
                jobDocument["mask"] = {"firstevent": job["mask"]["FirstEvent"],
                                       "lastevent": job["mask"]["LastEvent"],
                                       "firstlumi": job["mask"]["FirstLumi"],
                                       "lastlumi": job["mask"]["LastLumi"],
                                       "firstrun": job["mask"]["FirstRun"],
                                       "lastrun": job["mask"]["LastRun"]}
                jobDocument["name"] = job["name"]
                jobDocument["type"] = "job"

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
        """
        _createDatabase_

        """
        database = self.couchdb.createDatabase(self.dbname)
        
        hashViewDoc = database.createDesignDoc("JobDump")
        hashViewDoc["views"] = {"stateTransitionsByJobID": stateTransitionsByJobID,
                                "fwjrsByJobID": fwjrsByJobID,
                                "jobsByJobID": jobsByJobID}
     
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
        dao = self.daofactory(classname = "Jobs.ChangeState")
        dao.execute(jobs, conn = self.getDBConn(),
                    transaction = self.existingTransaction())
