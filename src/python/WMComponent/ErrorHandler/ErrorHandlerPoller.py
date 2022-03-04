#!/usr/bin/env python
"""
The actual error handler algorithm

The current ErrorHandler will either exhaust jobs based on the retry_count, or move jobs
onward into their necessary cooloff state.  The number of retries a job is allowed is
based upon:

config.ErrorHandler.maxRetries

However, it can also be used to handle jobs based on properties in the FWJR.
In order to engage any of this behavior you have to set the config option:
config.ErrorHandler.readFWJR = True

It will then take three arguments.

config.ErrorHandler.maxFailTime:  This should be a time in seconds after which, if the job took that long to fail,
it should be exhausted immediately.  It defaults to 24 hours.

config.ErrorHandler.passExitCodes:  This should be a list of exitCodes that you want to cause the job to move
immediately to the 'created' state, skipping cooloff.  It defaults to [].

Note that exitCodesNoRetry has precedence over passExitCodes.
"""
from future import standard_library
standard_library.install_aliases()

import logging
import os.path
import threading
from http.client import HTTPException
from Utils.Timers import timeFunction
from Utils.IteratorTools import grouper
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.DAOFactory import DAOFactory
from WMCore.Database.CouchUtils import CouchConnectionError
from WMCore.FwkJobReport.Report import Report
from WMCore.JobStateMachine.ChangeState import ChangeState
from WMCore.WMBS.Job import Job
from WMCore.WMException import WMException
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.Services.ReqMgrAux.ReqMgrAux import ReqMgrAux


class ErrorHandlerException(WMException):
    """
    The Exception class for the ErrorHandlerPoller

    """
    pass


class ErrorHandlerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)
        self.changeState = ChangeState(self.config)

        if hasattr(self.config, "Tier0Feeder"):
            self.reqAuxDB = None
            self.maxRetries = self.config.ErrorHandler.maxRetries
        else:
            cacheduration = getattr(self.config.General, "ReqMgrAuxCacheDuration", 5 / 60)  # 5 minutes
            self.reqAuxDB = ReqMgrAux(self.config.General.ReqMgr2ServiceURL, httpDict={'cacheduration': cacheduration})

        self.exitCodesNoRetry = []
        self.maxProcessSize = getattr(self.config.ErrorHandler, 'maxProcessSize', 250)
        self.maxFailTime = getattr(self.config.ErrorHandler, 'maxFailTime', 32 * 3600)
        self.readFWJR = getattr(self.config.ErrorHandler, 'readFWJR', False)
        self.passCodes = getattr(self.config.ErrorHandler, 'passExitCodes', [])

        self.getJobs = self.daoFactory(classname="Jobs.GetAllJobs")
        self.idLoad = self.daoFactory(classname="Jobs.LoadFromIDWithType")
        self.loadAction = self.daoFactory(classname="Jobs.LoadForErrorHandler")

        self.dataCollection = DataCollectionService(url=config.ACDC.couchurl,
                                                    database=config.ACDC.database)

        self.setupComponentParam()

        return

    def setupComponentParam(self):
        """
        Initialize (and update every cycle) some of the component's
        parameters, according to the agent type (T0/Production) and agent config.
        :return: True if the setup was completed, False if any non-critical problem happened
        """
        if self.reqAuxDB:
            agentConfig = self.reqAuxDB.getWMAgentConfig(self.config.Agent.hostName)
            if not agentConfig:
                return False

            self.exitCodesNoRetry = agentConfig.get("NoRetryExitCodes", [])
            if agentConfig.get("UserDrainMode") and agentConfig.get("SpeedDrainMode") \
                and agentConfig.get("SpeedDrainConfig")['NoJobRetries']['Enabled']:
                logging.info("Agent is in speed drain mode. Not retrying any jobs.")
                self.maxRetries = 0
            else:
                self.maxRetries = agentConfig.get("MaxRetries")

        if not isinstance(self.maxRetries, dict):
            self.maxRetries = {'default': self.maxRetries}
        if 'default' not in self.maxRetries:
            raise ErrorHandlerException('Max retries for the default job type must be specified')
        return True

    def setup(self, parameters=None):
        """
        Load DB objects required for queries
        """
        # For now, does nothing
        return

    def terminate(self, params):
        """
        _terminate_

        Do one pass, then commit suicide
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)

    def exhaustJobs(self, jobList):
        """
        _exhaustJobs_

        Actually do the jobs exhaustion
        """

        # Remove all the files in the exhausted jobs.
        logging.debug("About to fail input files for exhausted jobs")
        for job in jobList:
            job.failInputFiles()

        # Do not build ACDC for utilitarian job types
        acdcJobList = [job for job in jobList if job['type'] not in ['LogCollect', 'Cleanup']]

        self.handleACDC(acdcJobList)

        self.changeState.propagate(jobList, 'exhausted', 'retrydone')

        return

    def processRetries(self, jobList, state):
        """
        _processRetries_

        Actually do the retries
        """
        logging.info("Processing retries for %d failed jobs of type %sfailed", len(jobList), state)
        retrydoneJobs = []
        cooloffJobs = []
        passJobs = []

        if not isinstance(self.maxRetries, dict):
            self.maxRetries = {'default': self.maxRetries}
        if 'default' not in self.maxRetries:
            raise ErrorHandlerException('Max retries for the default job type must be specified')

        # Retries < max retry count
        for job in jobList:
            allowedRetries = self.maxRetries.get(job['type'], self.maxRetries['default'])
            # Retries < allowed max retry count
            if job['retry_count'] < allowedRetries and state != 'create':
                cooloffJobs.append(job)
            # Check if Retries >= allowed max retry count
            elif job['retry_count'] >= allowedRetries or state == 'create':
                retrydoneJobs.append(job)
                msg = "Stopping retries for job %d" % job['id']
                logging.debug(msg)
                logging.debug("JobInfo: %s", job)

        if self.readFWJR:
            # Then we have to check each FWJR for exit status
            cooloffJobs, passJobs, retrydoneFWJRJobs = self.readFWJRForErrors(cooloffJobs)
            retrydoneJobs.extend(retrydoneFWJRJobs)

        # Now to actually do something.
        logging.debug("About to propagate jobs")
        if len(retrydoneJobs) > 0:
            self.changeState.propagate(retrydoneJobs, 'retrydone',
                                       '%sfailed' % state, updatesummary=True)
        if len(cooloffJobs) > 0:
            self.changeState.propagate(cooloffJobs, '%scooloff' % state,
                                       '%sfailed' % state, updatesummary=True)
        if len(passJobs) > 0:
            # Overwrite the transition states and move directly to created
            self.changeState.propagate(passJobs, 'created', 'new')

        return

    def handleACDC(self, jobList):
        """
        _handleACDC_

        Do the ACDC creation and hope it works
        """
        idList = [x['id'] for x in jobList]
        logging.info("Starting to build ACDC with %i jobs", len(idList))
        logging.info("This operation will take some time...")
        loadList = self.loadJobsFromListFull(idList)
        for job in loadList:
            job.getMask()
        self.dataCollection.failedJobs(loadList)
        return

    def readFWJRForErrors(self, jobList):
        """
        _readFWJRForErrors_

        Check the FWJRs of the failed jobs
        and determine those that can be retried
        and which must be retried without going through cooloff.
        Returns a triplet with cooloff, passed and exhausted jobs.
        """
        cooloffJobs = []
        passJobs = []
        exhaustJobs = []

        for job in jobList:
            report = Report()
            reportPath = job['fwjr_path']
            if reportPath is None:
                logging.error("No FWJR in job %i, ErrorHandler can't process it.\n Passing it to cooloff.", job['id'])
                cooloffJobs.append(job)
                continue
            if not os.path.isfile(reportPath):
                logging.error(
                    "Failed to find FWJR for job %i in location %s.\n Passing it to cooloff.", job['id'], reportPath)
                cooloffJobs.append(job)
                continue
            try:
                report.load(reportPath)
                # First let's check the time conditions
                times = report.getFirstStartLastStop()
                startTime = None
                stopTime = None
                if times is not None:
                    startTime = times['startTime']
                    stopTime = times['stopTime']

                # correct the location if the original location is different from recorded in wmbs
                # WARNING: we are not updating job location in wmbs only updating in couchdb by doing this.
                # If location in wmbs needs to be updated, it should happen in JobAccountant.
                locationFromFWJR = report.getSiteName()
                if locationFromFWJR:
                    job["location"] = locationFromFWJR
                    job["site_cms_name"] = locationFromFWJR

                if startTime is None or stopTime is None:
                    # We have no information to make a decision, keep going.
                    logging.debug("No start, stop times for steps for job %i", job['id'])
                elif stopTime - startTime > self.maxFailTime:
                    msg = "Job %i exhausted after running on node for %i seconds" % (job['id'], stopTime - startTime)
                    logging.debug(msg)
                    exhaustJobs.append(job)
                    continue

                if len([x for x in report.getExitCodes() if x in self.exitCodesNoRetry]):
                    msg = "Job %i exhausted due to a bad exit code (%s)" % (job['id'], str(report.getExitCodes()))
                    logging.debug(msg)
                    exhaustJobs.append(job)
                    continue

                if len([x for x in report.getExitCodes() if x in self.passCodes]):
                    msg = "Job %i restarted immediately due to an exit code (%s)" % (job['id'],
                                                                                     str(report.getExitCodes()))
                    logging.debug(msg)
                    passJobs.append(job)
                    continue

                cooloffJobs.append(job)

            except Exception as ex:
                logging.warning("Exception while trying to check jobs for failures!")
                logging.warning(str(ex))
                logging.warning("Ignoring and sending job to cooloff")
                cooloffJobs.append(job)

        return cooloffJobs, passJobs, exhaustJobs

    def handleRetryDoneJobs(self, jobList):
        """
        _handleRetryDoneJobs_

        """
        myThread = threading.currentThread()
        logging.info("About to process %d retry done jobs", len(jobList))
        myThread.transaction.begin()
        self.exhaustJobs(jobList)
        myThread.transaction.commit()

        return

    def handleFailedJobs(self, jobList, state):
        """
        _handleFailedJobs_

        """
        myThread = threading.currentThread()
        logging.info("About to process %d failures", len(jobList))
        myThread.transaction.begin()
        self.processRetries(jobList, state)
        myThread.transaction.commit()

        return

    def handleErrors(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Run over created, submitted and executed job failures
        failure_states = ['create', 'submit', 'job']
        for state in failure_states:
            idList = self.getJobs.execute(state="%sfailed" % state)
            logging.info("Found %d failed jobs in state %sfailed", len(idList), state)
            for jobSlice in grouper(idList, self.maxProcessSize):
                jobList = self.loadJobsFromList(jobSlice)
                self.handleFailedJobs(jobList, state)

        # Run over jobs done with retries
        idList = self.getJobs.execute(state='retrydone')
        logging.info("Found %d jobs done with all retries", len(idList))
        for jobSlice in grouper(idList, self.maxProcessSize):
            jobList = self.loadJobsFromList(jobSlice)
            self.handleRetryDoneJobs(jobList)

        return

    def loadJobsFromList(self, idList):
        """
        _loadJobsFromList_

        Load jobs in bulk
        """
        binds = []
        for jobID in idList:
            binds.append({"jobid": jobID})
        results = self.idLoad.execute(jobID=binds)

        # You have to have a list
        if isinstance(results, dict):
            results = [results]

        listOfJobs = []
        for entry in results:
            # One job per entry
            tmpJob = Job(id=entry['id'])
            tmpJob.update(entry)
            listOfJobs.append(tmpJob)

        return listOfJobs

    def loadJobsFromListFull(self, idList):
        """
        _loadJobsFromList_

        Load jobs in bulk.
        Include the full metadata.
        """

        binds = []
        for jobID in idList:
            binds.append({"jobid": jobID})

        results = self.loadAction.execute(jobID=binds)

        # You have to have a list
        if isinstance(results, dict):
            results = [results]

        listOfJobs = []
        for entry in results:
            # One job per entry
            tmpJob = Job(id=entry['id'])
            tmpJob.update(entry)
            listOfJobs.append(tmpJob)

        return listOfJobs

    @timeFunction
    def algorithm(self, parameters=None):
        """
        Performs the handleErrors method, looking for each type of failure
        And deal with it as desired.
        """
        logging.debug("Running error handling algorithm")
        if not self.setupComponentParam():
            msg = "Component failed to retrieve agent configuration from central ReqMgr Aux DB."
            msg += " Skipping this cycle."
            logging.error(msg)
            return

        try:
            myThread = threading.currentThread()
            self.handleErrors()
        except (CouchConnectionError, HTTPException) as ex:
            if getattr(myThread, 'transaction', None) is not None:
                myThread.transaction.rollback()
            msg = "Caught CouchConnectionError/HTTPException exception in ErrorHandler. "
            msg += "Transactions postponed until the next polling cycle\n"
            msg += str(ex)
            logging.error(msg)
        except Exception as ex:
            if getattr(myThread, 'transaction', None) is not None:
                myThread.transaction.rollback()
            msg = "Caught unexpected exception in ErrorHandler:\n"
            msg += str(ex)
            logging.exception(msg)
            raise ErrorHandlerException(msg)
