#!/usr/bin/env python
"""
_ChangeState_

Propagate a job from one state to another.
"""

from builtins import str
import logging
import re
import time
import traceback

from WMCore.DataStructs.WMObject import WMObject
from WMCore.Database.CMSCouch import CouchNotFoundError, CouchError
from WMCore.Database.CMSCouch import CouchServer
from WMCore.JobStateMachine.SummaryDB import updateSummaryDB
from WMCore.JobStateMachine.Transitions import Transitions
from WMCore.Lexicon import sanitizeURL
from WMCore.WMConnectionBase import WMConnectionBase
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

CMSSTEP = re.compile(r'^cmsRun[0-9]+$')


def discardConflictingDocument(couchDbInstance, data, result):
    """
    _discardConflictingDocument_

    This should be passed to the queue and commit calls of CMSCouch
    in order to tell it what to do with conflicting documents.
    In this case we trash the old one and replace with what we were
    trying to commit, this is available in the data argument.
    And the result tells us the id of the conflicting document
    """

    conflictingId = result["id"]

    try:
        if not couchDbInstance.documentExists(conflictingId):
            # It doesn't exist, this is odd
            # Don't try again
            return result

        # Get the revision to override
        originalDocRev = couchDbInstance.document(conflictingId)["_rev"]

        # Look for the data to be commited
        retval = result
        for doc in data["docs"]:
            if doc["_id"] == conflictingId:
                doc["_rev"] = originalDocRev
                retval = couchDbInstance.commitOne(doc)
                break

        return retval
    except CouchError as ex:
        logging.error("Couldn't resolve conflict when updating document with id %s", result["id"])
        logging.error("Error: %s", str(ex))
        return result


def getDataFromSpecFile(specFile):
    workload = WMWorkloadHelper()
    workload.load(specFile)
    campaign = workload.getCampaign()
    result = {"Campaign": campaign}
    for task in workload.taskIterator():
        result[task.getPathName()] = task.getPrepID()
    return result


class ChangeState(WMObject, WMConnectionBase):
    """
    Propagate the state of a job through the JSM.
    """

    def __init__(self, config, couchDbName=None):
        WMObject.__init__(self, config)
        WMConnectionBase.__init__(self, "WMCore.WMBS")

        if couchDbName is None:
            self.dbname = getattr(self.config.JobStateMachine, "couchDBName")
        else:
            self.dbname = couchDbName

        self.jobsdatabase = None
        self.fwjrdatabase = None
        self.jsumdatabase = None
        self.statsumdatabase = None

        self.couchdb = CouchServer(self.config.JobStateMachine.couchurl)
        self._connectDatabases()

        self.getCouchDAO = self.daofactory("Jobs.GetCouchID")
        self.setCouchDAO = self.daofactory("Jobs.SetCouchID")
        self.incrementRetryDAO = self.daofactory("Jobs.IncrementRetry")
        self.workflowTaskDAO = self.daofactory("Jobs.GetWorkflowTask")
        self.jobTypeDAO = self.daofactory("Jobs.GetType")
        self.updateLocationDAO = self.daofactory("Jobs.UpdateLocation")
        self.getWorkflowSpecDAO = self.daofactory("Workflow.GetSpecAndNameFromTask")

        self.maxUploadedInputFiles = getattr(self.config.JobStateMachine, 'maxFWJRInputFiles', 1000)
        self.workloadCache = {}
        return

    def _connectDatabases(self):
        """
        Try connecting to the couchdbs
        """
        if not hasattr(self, 'jobsdatabase') or self.jobsdatabase is None:
            try:
                self.jobsdatabase = self.couchdb.connectDatabase("%s/jobs" % self.dbname, size=250)
            except Exception as ex:
                logging.error("Error connecting to couch db '%s/jobs': %s", self.dbname, str(ex))
                self.jobsdatabase = None
                return False

        if not hasattr(self, 'fwjrdatabase') or self.fwjrdatabase is None:
            try:
                self.fwjrdatabase = self.couchdb.connectDatabase("%s/fwjrs" % self.dbname, size=250)
            except Exception as ex:
                logging.error("Error connecting to couch db '%s/fwjrs': %s", self.dbname, str(ex))
                self.fwjrdatabase = None
                return False

        if not hasattr(self, 'jsumdatabase') or self.jsumdatabase is None:
            dbname = getattr(self.config.JobStateMachine, 'jobSummaryDBName')
            try:
                self.jsumdatabase = self.couchdb.connectDatabase(dbname, size=250)
            except Exception as ex:
                logging.error("Error connecting to couch db '%s': %s", dbname, str(ex))
                self.jsumdatabase = None
                return False

        if not hasattr(self, 'statsumdatabase') or self.statsumdatabase is None:
            dbname = getattr(self.config.JobStateMachine, 'summaryStatsDBName')
            try:
                self.statsumdatabase = self.couchdb.connectDatabase(dbname, size=250)
            except Exception as ex:
                logging.error("Error connecting to couch db '%s': %s", dbname, str(ex))
                self.jsumdatabase = None
                return False

        return True

    def propagate(self, jobs, newstate, oldstate, updatesummary=False):
        """
        Move the job from a state to another. Book keep the change to CouchDB.
        Report the information to the Dashboard.
        Take a list of job objects (dicts) and the desired state change.
        Return the jobs back, throw assertion error if the state change is not allowed
        and other exceptions as appropriate
        """
        if not isinstance(jobs, list):
            jobs = [jobs]

        if len(jobs) == 0:
            return

        # 1. Is the state transition allowed?
        self.check(newstate, oldstate)

        # 2. Load workflow/task information into the jobs
        self.loadExtraJobInformation(jobs)

        # 3. Make the state transition
        self.persist(jobs, newstate, oldstate)

        # 4. Complete the job information for jobs in created state
        try:
            self.completeCreatedJobsInformation(jobs, newstate, oldstate)
        except Exception as ex:
            logging.exception("Error complementing created job information: %s", str(ex))

        # 5. Document the state transition in couch
        try:
            self.recordInCouch(jobs, newstate, oldstate, updatesummary)
        except UnicodeDecodeError as ex:
            msg = "A critical error happened! Report it to developers. Error: %s" % str(ex)
            logging.exception(msg)
            raise
        except Exception as ex:
            logging.error("Error updating job in couch: %s", str(ex))
            logging.error(traceback.format_exc())

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

    def recordInCouch(self, jobs, newstate, oldstate, updatesummary=False):
        """
        _recordInCouch_

        Record relevant job information in couch. If the job does not yet exist
        in couch it will be saved as a seperate document.  If the job has a FWJR
        attached that will be saved as a seperate document.
        """
        if not self._connectDatabases():
            logging.error('Databases not connected properly')
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

            if couchDocID is None:
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
                    for key in job['mask']['runAndLumis']:
                        jobDocument['mask']['runAndLumis'][str(key)] = job['mask']['runAndLumis'][key]

                jobDocument["name"] = job["name"]
                jobDocument["type"] = "job"
                jobDocument["user"] = job.get("user", None)
                jobDocument["group"] = job.get("group", None)
                jobDocument["taskType"] = job.get("taskType", "Unknown")
                jobDocument["jobType"] = job.get("jobType", "Unknown")

                couchRecordsToUpdate.append({"jobid": job["id"],
                                             "couchid": jobDocument["_id"]})
                self.jobsdatabase.queue(jobDocument, callback=discardConflictingDocument)
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
                self.jobsdatabase.makeRequest(uri=updateUri, type="PUT", decode=False)

            # updating the status of the summary doc only when it is explicitely requested
            # doc is already in couch
            if updatesummary:
                jobSummaryId = job["name"]
                updateUri = "/" + self.jsumdatabase.name + "/_design/WMStatsAgent/_update/jobSummaryState/" + jobSummaryId
                # map retrydone state to jobfailed state for monitoring
                if newstate == "retrydone":
                    monitorState = "jobfailed"
                else:
                    monitorState = newstate
                updateUri += "?newstate=%s&timestamp=%s" % (monitorState, timestamp)
                self.jsumdatabase.makeRequest(uri=updateUri, type="PUT", decode=False)
                logging.debug("Updated job summary status for job %s", jobSummaryId)

                updateUri = "/" + self.jsumdatabase.name + "/_design/WMStatsAgent/_update/jobStateTransition/" + jobSummaryId
                updateUri += "?oldstate=%s&newstate=%s&location=%s&timestamp=%s" % (oldstate,
                                                                                    monitorState,
                                                                                    job["location"],
                                                                                    timestamp)
                self.jsumdatabase.makeRequest(uri=updateUri, type="PUT", decode=False)
                logging.debug("Updated job summary state history for job %s", jobSummaryId)

            if job.get("fwjr", None):

                cachedByWorkflow = self.workloadCache.setdefault(job['workflow'],
                                                                 getDataFromSpecFile(
                                                                     self.getWorkflowSpecDAO.execute(job['task'])[
                                                                         job['task']]['spec']))
                job['fwjr'].setCampaign(cachedByWorkflow.get('Campaign', ''))
                job['fwjr'].setPrepID(cachedByWorkflow.get(job['task'], ''))
                # If there are too many input files, strip them out
                # of the FWJR, as they should already
                # be in the database
                # This is not critical
                try:
                    if len(job['fwjr'].getAllInputFiles()) > self.maxUploadedInputFiles:
                        job['fwjr'].stripInputFiles()
                except Exception as ex:
                    logging.error("Error while trying to strip input files from FWJR.  Ignoring. : %s", str(ex))

                if newstate == "retrydone":
                    jobState = "jobfailed"
                else:
                    jobState = newstate

                # there is race condition updating couch record location and job is completed.
                # for the fast fail job, it could miss the location update
                job["location"] = job["fwjr"].getSiteName() or job.get("location", "Unknown")
                # complete fwjr document
                job["fwjr"].setTaskName(job["task"])
                jsonFWJR = job["fwjr"].__to_json__(None)

                # Don't archive cleanup job report
                if job["jobType"] == "Cleanup":
                    archStatus = "skip"
                else:
                    archStatus = "ready"

                fwjrDocument = {"_id": "%s-%s" % (job["id"], job["retry_count"]),
                                "jobid": job["id"],
                                "jobtype": job["jobType"],
                                "jobstate": jobState,
                                "retrycount": job["retry_count"],
                                "archivestatus": archStatus,
                                "fwjr": jsonFWJR,
                                "type": "fwjr"}
                self.fwjrdatabase.queue(fwjrDocument, timestamp=True, callback=discardConflictingDocument)

                updateSummaryDB(self.statsumdatabase, job)

                # TODO: can add config switch to swich on and off
                # if self.config.JobSateMachine.propagateSuccessJobs or (job["retry_count"] > 0) or (newstate != 'success'):
                if (job["retry_count"] > 0) or (newstate != 'success'):
                    jobSummaryId = job["name"]
                    # building a summary of fwjr
                    logging.debug("Pushing job summary for job %s", jobSummaryId)
                    errmsgs = {}
                    inputs = []
                    if "steps" in fwjrDocument["fwjr"]:
                        for step in fwjrDocument["fwjr"]["steps"]:
                            if "errors" in fwjrDocument["fwjr"]["steps"][step]:
                                errmsgs[step] = [error for error in fwjrDocument["fwjr"]["steps"][step]["errors"]]
                            if "input" in fwjrDocument["fwjr"]["steps"][step] and "source" in \
                                    fwjrDocument["fwjr"]["steps"][step]["input"]:
                                inputs.extend(
                                    [source["runs"] for source in fwjrDocument["fwjr"]['steps'][step]["input"]["source"]
                                     if "runs" in source])

                    outputs = []
                    outputDataset = None
                    for singlestep in job["fwjr"].listSteps():
                        for singlefile in job["fwjr"].getAllFilesFromStep(step=singlestep):
                            if singlefile:
                                if len(singlefile.get('locations', set())) > 1:
                                    locations = list(singlefile.get('locations'))
                                elif singlefile.get('locations'):
                                    locations = singlefile['locations'].pop()
                                else:
                                    locations = set()
                                if CMSSTEP.match(singlestep):
                                    outType = 'output'
                                else:
                                    outType = singlefile.get('module_label', None)
                                outputs.append({'type': outType,
                                                'lfn': singlefile.get('lfn', None),
                                                'location': locations,
                                                'checksums': singlefile.get('checksums', {}),
                                                'size': singlefile.get('size', None)})
                                # it should have one output dataset for all the files
                                outputDataset = singlefile.get('dataset', None) if not outputDataset else outputDataset
                    inputFiles = []
                    for inputFileStruct in job["fwjr"].getAllInputFiles():
                        # check if inputFileSummary needs to be extended
                        inputFileSummary = {}
                        inputFileSummary["lfn"] = inputFileStruct["lfn"]
                        inputFileSummary["input_type"] = inputFileStruct["input_type"]
                        inputFiles.append(inputFileSummary)

                    # Don't record intermediate jobfailed status in the jobsummary
                    # change to jobcooloff which will be overwritten by error handler anyway
                    if (job["retry_count"] > 0) and (newstate == 'jobfailed'):
                        summarystate = 'jobcooloff'
                    else:
                        summarystate = newstate

                    jobSummary = {"_id": jobSummaryId,
                                  "wmbsid": job["id"],
                                  "type": "jobsummary",
                                  "retrycount": job["retry_count"],
                                  "workflow": job["workflow"],
                                  "task": job["task"],
                                  "jobtype": job["jobType"],
                                  "state": summarystate,
                                  "site": job.get("location", None),
                                  "cms_location": job["fwjr"].getSiteName(),
                                  "exitcode": job["fwjr"].getExitCode(),
                                  "eos_log_url": job["fwjr"].getLogURL(),
                                  "worker_node_info": job["fwjr"].getWorkerNodeInfo(),
                                  "errors": errmsgs,
                                  "lumis": inputs,
                                  "outputdataset": outputDataset,
                                  "inputfiles": inputFiles,
                                  "acdc_url": "%s/%s" % (
                                  sanitizeURL(self.config.ACDC.couchurl)['url'], self.config.ACDC.database),
                                  "agent_name": self.config.Agent.hostName,
                                  "output": outputs}
                    if couchDocID is not None:
                        try:
                            currentJobDoc = self.jsumdatabase.document(id=jobSummaryId)
                            jobSummary['_rev'] = currentJobDoc['_rev']
                            jobSummary['state_history'] = currentJobDoc.get('state_history', [])
                            # record final status transition
                            if newstate == 'success':
                                finalStateDict = {'oldstate': oldstate,
                                                  'newstate': newstate,
                                                  'location': job["location"],
                                                  'timestamp': timestamp}
                                jobSummary['state_history'].append(finalStateDict)

                            noEmptyList = ["inputfiles", "lumis"]
                            for prop in noEmptyList:
                                jobSummary[prop] = jobSummary[prop] if jobSummary[prop] else currentJobDoc.get(prop, [])
                        except CouchNotFoundError:
                            pass
                    self.jsumdatabase.queue(jobSummary, timestamp=True)

        if len(couchRecordsToUpdate) > 0:
            self.setCouchDAO.execute(bulkList=couchRecordsToUpdate,
                                     conn=self.getDBConn(),
                                     transaction=self.existingTransaction())

        self.jobsdatabase.commit(callback=discardConflictingDocument)
        self.fwjrdatabase.commit(callback=discardConflictingDocument)
        self.jsumdatabase.commit()
        return

    def persist(self, jobs, newstate, oldstate):
        """
        _persist_

        Update the job state in the database.
        """

        if newstate == "killed":
            self.incrementRetryDAO.execute(jobs, increment=99999,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
        elif oldstate == "submitcooloff" or oldstate == "jobcooloff" or oldstate == "createcooloff":
            self.incrementRetryDAO.execute(jobs,
                                           conn=self.getDBConn(),
                                           transaction=self.existingTransaction())
        for job in jobs:
            job['state'] = newstate
            job['oldstate'] = oldstate

        dao = self.daofactory(classname="Jobs.ChangeState")
        dao.execute(jobs, conn=self.getDBConn(),
                    transaction=self.existingTransaction())

    def loadExtraJobInformation(self, jobs):
        # This is needed for both couch and dashboard
        jobIDsToCheck = []
        jobTasksToCheck = []
        # This is for mapping ids to the position in the list
        jobMap = {}
        for idx, job in enumerate(jobs):
            if job["couch_record"] is None:
                jobIDsToCheck.append(job["id"])
            if job.get("task", None) is None or job.get("workflow", None) is None \
                    or job.get("taskType", None) is None or job.get("jobType", None) is None:
                jobTasksToCheck.append(job["id"])
            jobMap[job["id"]] = idx

        if len(jobIDsToCheck) > 0:
            couchIDs = self.getCouchDAO.execute(jobID=jobIDsToCheck,
                                                conn=self.getDBConn(),
                                                transaction=self.existingTransaction())
            for couchID in couchIDs:
                idx = jobMap[couchID["jobid"]]
                jobs[idx]["couch_record"] = couchID["couch_record"]
        if len(jobTasksToCheck) > 0:
            jobTasks = self.workflowTaskDAO.execute(jobIDs=jobTasksToCheck,
                                                    conn=self.getDBConn(),
                                                    transaction=self.existingTransaction())
            for jobTask in jobTasks:
                idx = jobMap[jobTask["id"]]
                jobs[idx]["task"] = jobTask["task"]
                jobs[idx]["workflow"] = jobTask["name"]
                jobs[idx]["taskType"] = jobTask["type"]
                jobs[idx]["jobType"] = jobTask["subtype"]

    def completeCreatedJobsInformation(self, jobs, newstate, oldstate):
        """
        This method adds some extra information to the job object, if its
        new state is 'created' and its was in 'cooloff'.
        This is required for jobs coming from ErrorHandler, RetryManager
        or from the T0 pause script, where standard information of a
        WMBSJob is provided.
        :param jobs: list of job objects (dictionaries)
        :param newstate: string with the new state for this set of jobs
        :param oldstate: string with the previous state for this set of jobs
        :return: updates the job objects in-place
        """
        if newstate != 'created':
            return

        incrementRetry = True if 'cooloff' in oldstate else False
        for job in jobs:
            # It there's no jobID in the mask then it's not loaded
            if "jobID" not in job["mask"]:
                # Make sure the daofactory was not stripped
                if getattr(job["mask"], "daofactory", None):
                    job["mask"].load(jobID=job["id"])
            # If the mask is event based, then we have info to report
            if job["mask"]['inclusivemask'] and job["mask"]["LastEvent"] is not None and \
                            job["mask"]["FirstEvent"] is not None:
                job["nEventsToProc"] = int(job["mask"]["LastEvent"] -
                                           job["mask"]["FirstEvent"] + 1)
            # Increment retry when commanded
            if incrementRetry:
                job["retry_count"] += 1

    def recordLocationChange(self, jobs):
        """
        _recordLocationChange_

        Record a location change in couch and WMBS,
        this expects a list of dictionaries with
        jobid and location keys which represent
        the job id in WMBS and new location respectively.
        """
        if not self._connectDatabases():
            logging.error('Databases not connected properly')
            return

        # First update safely in WMBS
        self.updateLocationDAO.execute(jobs, conn=self.getDBConn(),
                                       transaction=self.existingTransaction())
        # Now try couch, this can fail and we don't require it to succeed
        try:
            jobIDs = [x['jobid'] for x in jobs]
            couchIDs = self.getCouchDAO.execute(jobIDs, conn=self.getDBConn(),
                                                transaction=self.existingTransaction())
            locationCache = dict((x['jobid'], x['location']) for x in jobs)
            for entry in couchIDs:
                couchRecord = entry['couch_record']
                location = locationCache[entry['jobid']]
                updateUri = "/" + self.jobsdatabase.name + "/_design/JobDump/_update/locationTransition/" + couchRecord
                updateUri += "?location=%s" % (location)
                self.jobsdatabase.makeRequest(uri=updateUri, type="PUT", decode=False)
        except Exception as ex:
            logging.error("Error updating job in couch: %s", str(ex))
            logging.error(traceback.format_exc())
