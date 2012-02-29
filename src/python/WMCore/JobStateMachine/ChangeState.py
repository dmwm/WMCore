#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

import time
import logging
import traceback

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import CouchConflictError
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
            self.dbname = getattr(self.config.JobStateMachine, "couchDBName")
        else:
            self.dbname = couchDbName

        try:
            self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
            self.jobsdatabase = self.couchdb.connectDatabase("%s/jobs" % self.dbname)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.dbname)
            self.jsumdatabase = self.couchdb.connectDatabase( getattr(self.config.JobStateMachine, 'jobSummaryDBName') )
        except Exception, ex:
            logging.error("Error connecting to couch: %s" % str(ex))
            self.jobsdatabase = None
            self.fwjrdatabase = None            
            self.jsumdatabase = None

        self.getCouchDAO = self.daofactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daofactory("Jobs.SetCouchID")
        self.incrementRetryDAO = self.daofactory("Jobs.IncrementRetry")
        self.workflowTaskDAO = self.daofactory("Jobs.GetWorkflowTask")

        self.maxUploadedInputFiles = getattr(self.config.JobStateMachine, 'maxFWJRInputFiles', 1000)
        return

    def propagate(self, jobs, newstate, oldstate, updatesummary = False):
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
            self.recordInCouch(jobs, newstate, oldstate, updatesummary)
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

    def recordInCouch(self, jobs, newstate, oldstate, updatesummary = False):
        """
        _recordInCouch_

        Record relevant job information in couch. If the job does not yet exist
        in couch it will be saved as a seperate document.  If the job has a FWJR
        attached that will be saved as a seperate document.
        """
        if not self.jobsdatabase or not self.fwjrdatabase:
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
                
            if job.get("site_cms_name", None):
                if newstate == "executing":
                    jobLocation = job["site_cms_name"]
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
                    docInputFile = inputFile.json()
                    
                    docInputFile["parents"] = []
                    for parent in inputFile["parents"]:
                        docInputFile["parents"].append({"lfn": parent["lfn"]})

                    jobDocument["inputfiles"].append(docInputFile)

                jobDocument["states"] = {"0": {"oldstate": oldstate,
                                               "newstate": newstate,
                                               "location": jobLocation,
                                               "timestamp": timestamp}}
                
                jobDocument["jobgroup"] = job["jobgroup"]
                jobDocument["mask"] = {"FirstEvent": job["mask"]["FirstEvent"],
                                       "LastEvent": job["mask"]["LastEvent"],
                                       "FirstLumi": job["mask"]["FirstLumi"],
                                       "LastLumi": job["mask"]["LastLumi"],
                                       "FirstRun": job["mask"]["FirstRun"],
                                       "LastRun": job["mask"]["LastRun"]}

                if job['mask']['runAndLumis'] != {}:
                    # Then we have to save the mask runAndLumis
                    jobDocument['mask']['runAndLumis'] = {}
                    for key in job['mask']['runAndLumis'].keys():
                        jobDocument['mask']['runAndLumis'][str(key)] = job['mask']['runAndLumis'][key]
                        
                jobDocument["name"] = job["name"]
                jobDocument["type"] = "job"
                jobDocument["user"] = job.get("user", None)
                jobDocument["group"] = job.get("group", None)
                jobDocument["taskType"] = job.get("taskType", "Unknown")
                jobDocument["jobType"] = job.get("jobType", "Unknown")

                couchRecordsToUpdate.append({"jobid": job["id"],
                                             "couchid": jobDocument["_id"]})                
                self.jobsdatabase.queue(jobDocument)
            else:
                # We send a PUT request to the stateTransition update handler.
                # Couch expects the parameters to be passed as arguments to in
                # the URI while the Requests class will only encode arguments
                # this way for GET requests.  Changing the Requests class to
                # encode PUT arguments as couch expects broke a bunch of code so
                # we'll just do our own encoding here.
                updateUri = "/" + self.jobsdatabase.name + "/_design/JobDump/_update/stateTransition/" + couchDocID
                updateUri += "?oldstate=%s&newstate=%s&location=%s&timestamp=%s" % (oldstate,
                                                                                    newstate,
                                                                                    jobLocation,
                                                                                    timestamp)
                self.jobsdatabase.makeRequest(uri = updateUri, type = "PUT", decode = False)

            # updating the status of the summary doc only when it is explicitely requested
            # doc is already in couch
            if updatesummary:
                # TODO change this to remove retry-count
                # jobSummaryId = "%s-%s" % (job["name"], job["retry_count"])
                jobSummaryId = job["name"]
                updateUri = "/" + self.jsumdatabase.name + "/_design/WMStats/_update/jobSummaryState/" + jobSummaryId
                updateUri += "?newstate=%s&timestamp=%s" % (newstate, timestamp)
                self.jsumdatabase.makeRequest(uri = updateUri, type = "PUT", decode = False)
                logging.debug("Updated job summary status for job %s" % jobSummaryId)

            if job.get("fwjr", None):

                # If there are too many input files, strip them out
                # of the FWJR, as they should already
                # be in the database
                # This is not critical
                try:
                    if len(job['fwjr'].getAllInputFiles()) > self.maxUploadedInputFiles:
                        job['fwjr'].stripInputFiles()
                except:
                    logging.error("Error while trying to strip input files from FWJR.  Ignoring.")
                    pass

                # complete fwjr document
                # TODO change this to remove retry-count
                #jobSummaryId = "%s-%s" % (job["name"], job["retry_count"])

                job["fwjr"].setTaskName(job["task"])
                fwjrDocument = {"_id": "%s-%s" % (job["name"], job["retry_count"]),
                                "jobid": job["id"],
                                "retrycount": job["retry_count"],
                                "fwjr": job["fwjr"].__to_json__(None),
                                "type": "fwjr"}
                self.fwjrdatabase.queue(fwjrDocument, timestamp = True)

                jobSummaryId = job["name"]
                # building a summary of fwjr
                logging.debug("Pushing job summary for job %s" % jobSummaryId)
                errmsgs = {}
                inputs = []
                for step in fwjrDocument["fwjr"]["steps"]:
                    if "errors" in fwjrDocument["fwjr"]["steps"][step]:
                        errmsgs[step] = [error for error in fwjrDocument["fwjr"]["steps"][step]["errors"]]
                    if "input" in fwjrDocument["fwjr"]["steps"][step] and "source" in fwjrDocument["fwjr"]["steps"][step]["input"]:
                        inputs.extend( [source["runs"] for source in fwjrDocument["fwjr"]['steps'][step]["input"]["source"] if "runs" in source] )
                outputs = [ {'type': singlefile.get('module_label', None),
                             'lfn': singlefile.get('lfn', None),
                             'location': singlefile.get('locations', None),
                             'checksums': singlefile.get('checksums', {}),
                             'size': singlefile.get('size', None) } for singlefile in job["fwjr"].getAllFiles() if singlefile ]
                jobSummary = {"_id": jobSummaryId,
                              "type": "jobsummary",
                              "retrycount": job["retry_count"],
                              "workflow": job["workflow"],
                              "task": job["task"],
                              "state": newstate,
                              "site": job["fwjr"].getSiteName(),
                              "exitcode": job["fwjr"].getExitCode(),
                              "errors": errmsgs,
                              "lumis": inputs,
                              "output": outputs }
                if couchDocID is not None:
                    jobSummary['_rev'] = self.jsumdatabase.document(id = jobSummaryId)['_rev']
                self.jsumdatabase.queue(jobSummary, timestamp = True)

        if len(couchRecordsToUpdate) > 0:
            self.setCouchDAO.execute(bulkList = couchRecordsToUpdate,
                                     conn = self.getDBConn(),
                                     transaction = self.existingTransaction())
            
        self.jobsdatabase.commit()
        self.fwjrdatabase.commit()
        self.jsumdatabase.commit()
        return

    def persist(self, jobs, newstate, oldstate):
        """
        _persist_

        Update the job state in the database.
        """
        if oldstate == "submitcooloff" or oldstate == "jobcooloff":
            self.incrementRetryDAO.execute(jobs,
                                           conn = self.getDBConn(),
                                           transaction = self.existingTransaction())
        if newstate == "killed":
            self.incrementRetryDAO.execute(jobs, increment = 99999,
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
          - user
          - group
          - taskType
          - jobType
          - performance
        """
        updateBase = "/" + self.jobsdatabase.name + "/_design/JobDump/_update/dashboardReporting/"
        viewResults = self.jobsdatabase.loadView("JobDump", "jobsToReport")

        jobsToReport = []
        for viewResult in viewResults["rows"]:
            jobReport = {"performance": {},
                         "exitCode": 0}
            jobReport.update(viewResult["value"])

            fwjrResults = self.fwjrdatabase.loadView("FWJRDump", "jobsToReport",
                                                     options = {"startkey": [jobReport["id"], jobReport["retryCount"], 0],
                                                                "endkey": [jobReport["id"], jobReport["retryCount"], {}]})

            errorTime = None
            exitCode = 0
            for row in fwjrResults["rows"]:
                jobReport["performance"][row["value"][0]] = row["value"][2]

                errors = row["value"][3]
                if len(errors) > 0:
                    if errorTime == None or row["value"][1] < errorTime:
                        erorrTime = row["value"][1]
                        exitCode = errors[0]["exitCode"]

            jobReport["exitCode"] = exitCode
            del jobReport["index"]
            del jobReport["id"]
            jobsToReport.append(jobReport)

            updateUri = updateBase + str(viewResult["value"]["id"])
            updateUri += "?index=%s" % (viewResult["value"]["index"])
            try:
                self.jobsdatabase.makeRequest(uri = updateUri, type = "PUT", decode = False)
            except CouchConflictError, ex:
                # The document has been updated under our feet, ignore the error and we'll
                # update it on the next polling cycle.
                pass
            
        return jobsToReport
