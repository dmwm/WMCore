#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

import time
import logging
import traceback
import re

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Database.CMSCouch import CouchConflictError, CouchNotFoundError
from WMCore.DataStructs.WMObject import WMObject
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Services.UUID import makeUUID
from WMCore.Services.Dashboard.DashboardReporter import DashboardReporter
from WMCore.WMConnectionBase import WMConnectionBase

CMSSTEP = re.compile(r'^cmsRun[0-9]+$')

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
            self.jobsdatabase = self.couchdb.connectDatabase("%s/jobs" % self.dbname, size = 250)
            self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.dbname, size = 250)
            self.jsumdatabase = self.couchdb.connectDatabase( getattr(self.config.JobStateMachine, 'jobSummaryDBName'), size = 250 )
        except Exception, ex:
            logging.error("Error connecting to couch: %s" % str(ex))
            self.jobsdatabase = None
            self.fwjrdatabase = None
            self.jsumdatabase = None

        try:
            self.dashboardReporter = DashboardReporter(config)
        except Exception, ex:
            logging.error("Error setting up the \
                          dashboard reporter: %s" % str(ex))

        self.getCouchDAO = self.daofactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daofactory("Jobs.SetCouchID")
        self.incrementRetryDAO = self.daofactory("Jobs.IncrementRetry")
        self.workflowTaskDAO = self.daofactory("Jobs.GetWorkflowTask")
        self.jobTypeDAO = self.daofactory("Jobs.GetType")

        self.maxUploadedInputFiles = getattr(self.config.JobStateMachine, 'maxFWJRInputFiles', 1000)
        return

    def propagate(self, jobs, newstate, oldstate, updatesummary = False):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        Report the information to the Dashboard.
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

        # 2. Load workflow/task information into the jobs
        self.loadExtraJobInformation(jobs)

        # 3. Document the state transition in couch
        try:
            self.recordInCouch(jobs, newstate, oldstate, updatesummary)
        except Exception, ex:
            logging.error("Error updating job in couch: %s" % str(ex))
            logging.error(traceback.format_exc())

        # 4. Report the job transition to the dashboard
        try:
            self.reportToDashboard(jobs, newstate, oldstate)
        except Exception, ex:
            logging.error("Error reporting to the dashboard: %s" % str(ex))
            logging.error(traceback.format_exc())

        # 4. Make the state transition
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

        timestamp = int(time.time())
        couchRecordsToUpdate = []

        for job in jobs:
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
                job["fwjr"].setTaskName(job["task"])
                fwjrDocument = {"_id": "%s-%s" % (job["id"], job["retry_count"]),
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
                if "steps" in fwjrDocument["fwjr"]:
                    for step in fwjrDocument["fwjr"]["steps"]:
                        if "errors" in fwjrDocument["fwjr"]["steps"][step]:
                            errmsgs[step] = [error for error in fwjrDocument["fwjr"]["steps"][step]["errors"]]
                        if "input" in fwjrDocument["fwjr"]["steps"][step] and "source" in fwjrDocument["fwjr"]["steps"][step]["input"]:
                            inputs.extend( [source["runs"] for source in fwjrDocument["fwjr"]['steps'][step]["input"]["source"] if "runs" in source] )

                outputs = []
                outputDataset = None
                for singlestep in job["fwjr"].listSteps():
                    for singlefile in job["fwjr"].getAllFilesFromStep(step=singlestep):
                        if singlefile:
                            outputs.append({'type': 'output' if CMSSTEP.match(singlestep) else singlefile.get('module_label', None),
                                            'lfn': singlefile.get('lfn', None),
                                            'location': list(singlefile.get('locations', set([]))) if len(singlefile.get('locations', set([]))) > 1
                                                                                                   else singlefile['locations'].pop(),
                                            'checksums': singlefile.get('checksums', {}),
                                            'size': singlefile.get('size', None) })
                            #it should have one output dataset for all the files
                            outputDataset = singlefile.get('dataset', None) if not outputDataset else outputDataset

                jobSummary = {"_id": jobSummaryId,
                              "wmbsid": job["id"],
                              "type": "jobsummary",
                              "retrycount": job["retry_count"],
                              "workflow": job["workflow"],
                              "task": job["task"],
                              "jobtype": job["jobType"],
                              "state": newstate,
                              "site": job["fwjr"].getSiteName(),
                              "exitcode": job["fwjr"].getExitCode(),
                              "errors": errmsgs,
                              "lumis": inputs,
                              "outputdataset": outputDataset,
                              "output": outputs }
                if couchDocID is not None:
                    try:
                        jobSummary['_rev'] = self.jsumdatabase.document(id = jobSummaryId)['_rev']
                    except CouchNotFoundError:
                        pass
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
        if oldstate == "submitcooloff" or oldstate == "jobcooloff" or oldstate == "createcooloff" :
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

    def reportToDashboard(self, jobs, newstate, oldstate):
        """
        _reportToDashboard_

        Report job information to the dashboard, completes the job dictionaries
        with any additional information needed
        """

        #If the new state is created it possible came from 3 locations:
        #JobCreator in that case it comes with all the needed info
        #ErrorHandler comes with the standard information of a WMBSJob
        #RetryManager comes with the standard information of a WMBSJob
        #Unpause script comes with the standard information of a WMBSJob
        #For those last 3 cases we need to fill the gaps
        if newstate == 'created':
            incrementRetry = True if 'cooloff' in oldstate else False
            self.completeCreatedJobsInformation(jobs, incrementRetry)
            self.dashboardReporter.handleCreated(jobs)
        #If the new state is executing that was done only by the JobSubmitter,
        #it sends jobs with select information, nevertheless is enough
        elif newstate == 'executing':
            statusMessage = 'Job was successfuly submitted'
            self.dashboardReporter.handleJobStatusChange(jobs, 'submitted',
                                                         statusMessage)
        #If the new state is success, then the JobAccountant sent the jobs.
        #Jobs come with all the standard information of a WMBSJob plus FWJR
        elif newstate == 'success':
            statusMessage = 'Job has completed successfully'
            self.dashboardReporter.handleJobStatusChange(jobs, 'succeeded',
                                                         statusMessage)
        elif newstate == 'jobfailed':
            #If it failed after being in complete state, then  the JobAccountant
            #sent the jobs, these come with all the standard information of a WMBSJob
            #plus FWJR
            if oldstate == 'complete':
                statusMessage = 'Job failed at the site'
            #If it failed while executing then it timed out in BossAir
            #The JobTracker should sent the jobs with the required information
            elif oldstate == 'executing':
                statusMessage = 'Job timed out in the agent'
            self.dashboardReporter.handleJobStatusChange(jobs, 'failed',
                                                        statusMessage)
        #In this case either a paused job was killed or the workqueue is killing
        #a workflow, in both cases a WMBSJob with all the info should come
        elif newstate == 'killed':
            if oldstate == 'jobpaused':
                statusMessage = 'A paused job was killed, maybe it is beyond repair'
            else:
                statusMessage = 'The whole workflow is being killed'
            self.dashboardReporter.handleJobStatusChange(jobs, 'killed',
                                                         statusMessage)

    def loadExtraJobInformation(self, jobs):
        #This is needed for both couch and dashboard
        jobIDsToCheck = []
        jobTasksToCheck = []
        #This is for mapping ids to the position in the list
        jobMap = {}
        for idx, job in enumerate(jobs):
            if job["couch_record"] == None:
                jobIDsToCheck.append(job["id"])
            if job.get("task", None) == None or job.get("workflow", None) == None \
                or job.get("taskType", None) == None or job.get("jobType", None) == None:
                jobTasksToCheck.append(job["id"])
            jobMap[job["id"]] = idx

        if len(jobIDsToCheck) > 0:
            couchIDs = self.getCouchDAO.execute(jobID = jobIDsToCheck,
                                                conn = self.getDBConn(),
                                                transaction = self.existingTransaction())
            for couchID in couchIDs:
                idx = jobMap[couchID["jobid"]]
                jobs[idx]["couch_record"] = couchID["couch_record"]
        if len(jobTasksToCheck) > 0:
            jobTasks = self.workflowTaskDAO.execute(jobIDs = jobTasksToCheck,
                                                    conn = self.getDBConn(),
                                                    transaction = self.existingTransaction())
            for jobTask in jobTasks:
                idx = jobMap[jobTask["id"]]
                jobs[idx]["task"] = jobTask["task"]
                jobs[idx]["workflow"] = jobTask["name"]
                jobs[idx]["taskType"] = jobTask["type"]
                jobs[idx]["jobType"]  = jobTask["subtype"]

    def completeCreatedJobsInformation(self, jobs, incrementRetry = False):
        for job in jobs:
            #It there's no jobID in the mask then it's not loaded
            if "jobID" not in job["mask"]:
                #Make sure the daofactory was not stripped
                if getattr(job["mask"], "daofactory", None):
                    job["mask"].load(jobID = job["id"])
            #If the mask is event based, then we have info to report
            if job["mask"]["LastEvent"] != None and \
               job["mask"]["FirstEvent"] != None and job["mask"]['inclusivemask']:
                job["nEventsToProc"] = int(job["mask"]["LastEvent"] -
                                            job["mask"]["FirstEvent"])
            #Increment retry when commanded
            if incrementRetry:
                job["retry_count"] += 1
