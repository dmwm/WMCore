#!/usr/bin/env python
"""
The actual jobTracker algorithm
"""
__all__ = []



import threading
import logging
import os
import os.path

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory
from WMCore.WMFactory         import WMFactory
from WMCore.WMException       import WMException

from WMCore.FwkJobReport.Report      import Report

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.BossAir.BossAirAPI          import BossAirAPI

class JobTrackerException(WMException):
    """
    _JobTrackerException_

    A job tracker exception-handling class for the JobTracker
    """

class JobTrackerPoller(BaseWorkerThread):
    """
    _JobTrackerPoller_

    Polls the BossAir database for complete jobs
    Handles completed jobs
    """


    def __init__(self, config):
        """
        Initialise class members
        """

        BaseWorkerThread.__init__(self)
        self.config = config

        myThread = threading.currentThread()


        self.changeState   = ChangeState(self.config)
        self.bossAir       = BossAirAPI(config = config)
        self.daoFactory    = DAOFactory(package = "WMCore.WMBS",
                                        logger = myThread.logger,
                                        dbinterface = myThread.dbi)

        self.jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")

        # initialize the alert framework (if available)
        self.initAlerts(compName = "JobTracker")


    def setup(self, parameters = None):
        """
        Load DB objects required for queries
        """

        return




    def terminate(self, params = None):
        """
        _terminate_

        Terminate the function after one more run.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return




    def algorithm(self, parameters = None):
        """
        Performs the archiveJobs method, looking for each type of failure
        And deal with it as desired.
        """
        logging.info("Running Tracker algorithm")
        myThread = threading.currentThread()
        try:
            self.trackJobs()
        except WMException as ex:
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollback()
            self.sendAlert(6, msg = str(ex))
            raise
        except Exception as ex:
            msg =  "Unknown exception in JobTracker!\n"
            msg += str(ex)
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollback()
            self.sendAlert(6, msg = msg)
            logging.error(msg)
            raise JobTrackerException(msg)

        return

    def trackJobs(self):
        """
        _trackJobs_

        Finds a list of running jobs and the sites that they're running at,
        and passes that off to tracking.
        """

        passedJobs = []
        failedJobs = []


        # Get all jobs WMBS thinks are running
        jobList = self.jobListAction.execute(state = "Executing")

        if jobList == []:
            # No jobs: do nothing
            return

        logging.info("Have list of %i executing jobs" % len(jobList))

        # Now get all jobs that BossAir thinks are complete
        completeJobs = self.bossAir.getComplete()

        logging.info("%i jobs are complete in BossAir" % (len(completeJobs)))
        logging.debug(completeJobs)

        for job in completeJobs:
            if not job['id'] in jobList:
                continue
            if job['status'].lower() == 'timeout':
                failedJobs.append(job)
            else:
                passedJobs.append(job)


        # Assume all these jobs "passed" if they aren't in timeout
        self.passJobs(passedJobs)
        self.failJobs(failedJobs)

        return


    def failJobs(self, failedJobs):
        """
        _failJobs_

        Dump those jobs that have failed due to timeout
        """

        myThread = threading.currentThread()

        if len(failedJobs) == 0:
            return

        myThread = threading.currentThread()


        # Load DAOs
        setFWJRAction = self.daoFactory(classname = "Jobs.SetFWJRPath")
        loadAction    = self.daoFactory(classname = "Jobs.LoadFromID")

        jrBinds = []

        for job in failedJobs:
            jrPath = os.path.join(job.getCache(),
                                  'Report.%i.pkl' % (job['retry_count']))
            jrBinds.append({'jobid': job['id'], 'fwjrpath': jrPath})
            #Make sure the job object goes packed with fwjr_path so it
            #can be persisted in couch

            fwjr = Report()
            try:
                fwjr.load(jrPath)
            except Exception:
                #Something went wrong reading the pickle
                logging.error("The pickle in %s could not be loaded, generating a new one" % jrPath)
                fwjr = Report()
                msg = "The job failed due to a timeout, unfortunately the original job report was lost"
                fwjr.addError("NoJobReport", 99303, "NoJobReport", msg)
                fwjr.save(jrPath)
            job["fwjr"] = fwjr

        # Set all paths at once
        myThread.transaction.begin()
        setFWJRAction.execute(binds = jrBinds)

        self.changeState.propagate(failedJobs, 'jobfailed', 'executing')
        logging.info("Failed %i jobs" % (len(failedJobs)))
        myThread.transaction.commit()


        return

    def passJobs(self, passedJobs):
        """
        _passJobs_

        Pass jobs and move their stuff?
        """

        if len(passedJobs) == 0:
            return

        #Mark 'em as complete
        loadAction    = self.daoFactory(classname = "Jobs.LoadFromID")
        setFWJRAction = self.daoFactory(classname = "Jobs.SetFWJRPath")

        jrBinds    = []
        for job in passedJobs:
            jrPath = os.path.join(job.getCache(),
                                  'Report.%i.pkl' % (job['retry_count']))
            jrBinds.append({'jobid': job['id'], 'fwjrpath': jrPath})

        myThread = threading.currentThread()
        myThread.transaction.begin()
        setFWJRAction.execute(binds = jrBinds, conn = myThread.transaction.conn,
                              transaction = True)
        myThread.transaction.commit()

        self.changeState.propagate(passedJobs, 'complete', 'executing')
        logging.debug("Propagating jobs in jobTracker")
        logging.info("Passed %i jobs" % len(passedJobs))
        return
