#!/usr/bin/env python
"""
The actual jobArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: JobArchiverPoller.py,v 1.4 2010/02/04 16:02:16 mnorman Exp $"
__version__ = "$Revision: 1.4 $"

import threading
import logging
import os
import os.path
import shutil

from subprocess import Popen, PIPE

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.JobStateMachine.ChangeState    import ChangeState

from WMCore.WMBS.Job          import Job
from WMCore.DAOFactory        import DAOFactory

class JobArchiverPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config
        self.changeState = ChangeState(self.config)

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        return




    def terminate(self, params):
        """
        _terminate_

        This function terminates the job after a final pass
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return


    

    def algorithm(self, parameters):
        """
	Performs the archiveJobs method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running algorithm for finding finished subscriptions")
        myThread = threading.currentThread()
        try:
            self.archiveJobs()
            self.pollForClosable()
        except:
            raise

        return


    def archiveJobs(self):
        """
        _archiveJobs_
        
        archiveJobs will handle the master task of looking for finished jobs,
        and running the code that cleans them out.
        """
        myThread = threading.currentThread()

        doneList  = self.findFinishedJobs()

        self.cleanWorkArea(doneList)

        successList = []
        failList    = []



        for job in doneList:
            if job["outcome"] == "success":
                successList.append(job)
            else:
                failList.append(job)
                
        myThread.transaction.begin()
        self.changeState.propagate(successList, "cleanout", "success")
        self.changeState.propagate(failList, "cleanout", "exhausted")
        myThread.transaction.commit()


    def findFinishedJobs(self):

        jobList = []

        jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList1  = jobListAction.execute(state = "success")
        jobList2  = jobListAction.execute(state = "exhausted")

        jobList.extend(jobList1)
        jobList.extend(jobList2)

        logging.error("Found jobs to be finished")
        logging.error(jobList)

        doneList = []
        
        for jobID in jobList:
            tmpJob = Job(id = jobID)
            tmpJob.load()
            doneList.append(tmpJob)


        return doneList


    def cleanWorkArea(self, doneList):
        """
        _cleanWorkArea_
        
        Upon workQueue realizing that a subscriptions is done, everything
        regarding those jobs is cleaned up.
        """

        myThread = threading.currentThread()

        for job in doneList:
            #print "About to clean cache for job %i" % (job['id'])
            self.cleanJobCache(job)
        
        return

    def cleanJobCache(self, job):
        """
        _cleanJobCache_

        Clears out any files still sticking around in the jobCache, tars up the contents and sends them off
        """

        myThread = threading.currentThread()

        cacheDir = job.getCache()

        if not cacheDir or not os.path.isdir(cacheDir):
            logging.error("Could not find jobCacheDir %s" % (cacheDir))
            return

        if os.listdir(cacheDir) == []:
            os.rmdir(cacheDir)
            return

        #Otherwise we have something in there
        tarName = 'Job_%s.tar' % (job['name'])
        tarString = ["tar"]
        tarString.append("-cvf")
        tarString.append('%s/%s' % (cacheDir, tarName))
        for fileName in os.listdir(cacheDir):
            tarString.append('%s' % (os.path.join(cacheDir, fileName)))

        #Now we should have all the files together.  Tar them up
        pipe = Popen(tarString, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.wait()

        shutil.move('%s/%s' % (cacheDir, tarName), '%s/%s' % (self.config.JobArchiver.logDir, tarName))

        shutil.rmtree('%s' % (cacheDir))

        #print "Job %i cleaned" % (job['id'])

        return


    def pollForClosable(self):
        """
        _pollForClosable_

        Search WMBS for filesets that can be closed and mark them as closed.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        closableFilesetDAO = self.daoFactory(classname = "Fileset.ListClosable")
        closableFilesets = closableFilesetDAO.execute()

        for closableFileset in closableFilesets:
            openFileset = Fileset(id = closableFileset)
            openFileset.load()

            logging.debug("Closing fileset %s" % openFileset.name)
            openFileset.markOpen(False)

        myThread.transaction.commit()

        
    
