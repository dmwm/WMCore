#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

import time
import logging
import traceback

from WMCore.Database.CMSCouch import CouchServer
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.WMConnectionBase import WMConnectionBase

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
            self.database = None

        self.getCouchDAO = self.daofactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daofactory("Jobs.SetCouchID")
        self.incrementRetryDAO = self.daofactory("Jobs.IncrementRetry")
        self.workflowTaskDAO = self.daofactory("Jobs.GetWorkflowTask")
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
            logging.error(traceback.format_exc())
            
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
               "Illegal state transition requested: %s -> %s" % (oldstate, newstate)

    def recordInCouch(self, jobs, newstate, oldstate):
        """
        _recordInCouch_

        Record relevant job information in couch. If the job does not yet exist
        in couch it will be saved as a seperate document.  If the job has a FWJR
        attached that will be saved as a seperate document.
        """
        if not self.database:
            return
        
        jobMap = {}
        jobIDsToCheck = []
        jobTasksToCheck = []
        for job in jobs:
            jobMap[job["id"]] = job
            if job["couch_record"] == None:
                jobIDsToCheck.append(job["id"])
            if job.get("task", None) == None or job.get("workflow", None) == None:
                jobTasksToCheck.append(job["id"])

        if len(jobIDsToCheck) > 0:
            couchIDs = self.getCouchDAO.execute(jobID = jobIDsToCheck,
                                                conn = self.getDBConn(),
                                                transaction = self.existingTransaction())
            for couchID in couchIDs:
                jobMap[couchID["jobid"]]["couch_record"] = couchID["couch_record"]
        if len(jobTasksToCheck) > 0:
            jobTasks = self.workflowTaskDAO.execute(jobIDs = jobTasksToCheck,
                                                    conn = self.getDBConn(),
                                                    transaction = self.existingTransaction())
            for jobTask in jobTasks:
                jobMap[jobTask["id"]]["task"] = jobTask["task"]
                jobMap[jobTask["id"]]["workflow"] = jobTask["name"]  

        timestamp = int(time.time())
        couchRecordsToUpdate = []
        
        for jobID in jobMap.keys():
            job = jobMap[jobID]
            couchDocID = job.get("couch_record", None)

            if newstate == "new":
                oldstate = "none"
                
            if job.get("location", None):
                if newstate == "executing":
                    jobLocation = job["location"]
                else:
                    jobLocation = "Agent"
            else:
                jobLocation = "Agent"
                
            if couchDocID == None:
                jobDocument = {}
                jobDocument["_id"] = str(job["id"])
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

                jobDocument["states"] = {"0": {"oldstate": oldstate,
                                               "newstate": newstate,
                                               "location": jobLocation,
                                               "timestamp": timestamp}}
                
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
            else:
                # We send a PUT request to the stateTransition update handler.
                # Couch expects the parameters to be passed as arguments to in
                # the URI while the Requests class will only encode arguments
                # this way for GET requests.  Changing the Requests class to
                # encode PUT arguments as couch expects broke a bunch of code so
                # we'll just do our own encoding here.
                updateUri = "/" + self.database.name + "/_design/JobDump/_update/stateTransition/" + couchDocID
                updateUri += "?oldstate=%s&newstate=%s&location=%s&timestamp=%s" % (oldstate,
                                                                                    newstate,
                                                                                    jobLocation,
                                                                                    timestamp)
                self.database.makeRequest(uri = updateUri, type = "PUT", decode = False)

            if job.get("fwjr", None):
                job["fwjr"].setTaskName(job["task"])
                fwjrDocument = {"jobid": job["id"],
                                "retrycount": job["retry_count"],
                                "fwjr": job["fwjr"].__to_json__(None),
                                "type": "fwjr"}
                self.database.queue(fwjrDocument, timestamp = True)

        if len(couchRecordsToUpdate) > 0:
            self.setCouchDAO.execute(bulkList = couchRecordsToUpdate,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
            
        self.database.commit()
        return

    def createDatabase(self):
        """
        _createDatabase_

        Create the couch database and install the views.
        """
        database = self.couchdb.createDatabase(self.dbname)
        database.commit()
        return database

    def persist(self, jobs, newstate, oldstate):
        """
        _persist_

        Update the job state in the database.
        """
        if oldstate == "submitcooloff" or oldstate == "jobcooloff":
            self.incrementRetryDAO.execute(jobs,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        
        for job in jobs:
            job['state'] = newstate
            job['oldstate'] = oldstate
            
        dao = self.daofactory(classname = "Jobs.ChangeState")
        dao.execute(jobs, conn = self.getDBConn(),
                    transaction = self.existingTransaction())

    def listTransitionsForDashboard(self):
        """
        _listTransitionsForDashboard_

        List information about jobs that have made transitions that need to be
        broadcast to bashboard.  This will return a list of dictionaries with
        the following keys:
          - name
          - retryCount
          - requestName
          - newState
          - oldState
        """
        updateBase = "/" + self.database.name + "/_design/JobDump/_update/dashboardReporting/"
        viewResults = self.database.loadView("JobDump", "jobsToReport")

        jobsToReport = []
        for viewResult in viewResults["rows"]:
            jobReport = {}
            jobReport.update(viewResult["value"])
            del jobReport["index"]
            del jobReport["id"]
            jobsToReport.append(jobReport)

            updateUri = updateBase + str(viewResult["value"]["id"])
            updateUri += "?index=%s" % (viewResult["value"]["index"])
            self.database.makeRequest(uri = updateUri, type = "PUT", decode = False)
            
        return jobsToReport
