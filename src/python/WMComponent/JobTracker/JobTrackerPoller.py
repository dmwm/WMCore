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

from WMCore.JobStateMachine.ChangeState import ChangeState

from WMCore.BossAir.BossAirAPI          import BossAirAPI

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
            myThread.transaction.begin()
            self.trackJobs()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

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


        # Now get all jobs that BossAir thinks are complete
        completeJobs = self.bossAir.getComplete()

        for job in completeJobs:
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
            #job.setFWJRPath(os.path.join(job.getCache(),
            #                             'Report.%i.pkl' % (job['retry_count'])))

        
        # Set all paths at once
        myThread.transaction.begin()
        setFWJRAction.execute(binds = jrBinds)

        print "Should be about to fail jobs"

        
        self.changeState.propagate(failedJobs, 'jobfailed', 'executing')
        myThread.transaction.commit()


        return

    def passJobs(self, passedJobs):
        """
        _passJobs_
        
        Pass jobs and move their stuff?
        """

        if len(passedJobs) == 0:
            return

        myThread = threading.currentThread()

        #Get their stuff?
        #I've got no idea how we want to do this.
        
        #Mark 'em as complete
        loadAction    = self.daoFactory(classname = "Jobs.LoadFromID")
        setFWJRAction = self.daoFactory(classname = "Jobs.SetFWJRPath")

        jrBinds    = []

        for job in passedJobs:
            jrPath = os.path.join(job.getCache(),
                                  'Report.%i.pkl' % (job['retry_count']))
            jrBinds.append({'jobid': job['id'], 'fwjrpath': jrPath})

        # Set all binds at once
        myThread.transaction.begin()
        setFWJRAction.execute(binds = jrBinds)


        logging.debug("Propagating jobs in jobTracker")
        self.changeState.propagate(passedJobs, 'complete', 'executing')
        myThread.transaction.commit()

        return
                


        
