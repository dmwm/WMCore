#!/usr/bin/env python
"""
The actual jobArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: JobArchiverPoller.py,v 1.1 2009/09/29 16:33:46 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import logging
import re
import os
import os.path
from sets import Set

from subprocess import Popen, PIPE

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset      import Fileset
from WMCore.WMBS.Workflow     import Workflow
from WMCore.WMBS.Job          import Job
from WMCore.WMFactory         import WMFactory
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
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)


        return




    def terminate(self,params):
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
        except:
            raise

        return


    def archiveJobs(self):
        """
        _archiveJobs_
        
        archiveJobs will handle the master task of looking for finished subscriptions,
        checking to see if they've finished, and then notifying the workQueue and
        finishing things up.
        """


        myThread = threading.currentThread()

        subList  = self.findFinishedSubscriptions()
        doneList = self.notifyWorkQueue(subList)
        self.cleanWorkArea(doneList)


    def findFinishedSubscriptions(self):
        """
        _findFinishedSubscriptions_
        
        Figures out which one of the subscriptions is actually finished.
        """
        subList = []

        myThread = threading.currentThread()

        myThread.transaction.begin()

        jobListAction = self.daoFactory(classname = "Jobs.GetAllJobs")
        jobList  = jobListAction.execute(state = "Closeout")

        subscriptionList = self.daoFactory(classname = "Subscriptions.List")
        subscriptions    = subscriptionList.execute()

        for subscription in subscriptions:
            wmbsSubscription = Subscription(id = subscription)
            jobs             = wmbsSubscription.getJobs()
            finished = True
            if len(jobs) == 0:
                finished = False
            for job in jobs:
                if not job['id'] in jobList:
                    finished = False
                    break
            if finished:
                subList.append(wmbsSubscription)

        myThread.transaction.commit()
                
        return subList

    def notifyWorkQueue(self, subList):
        """
        _notifyWorkQueue_
        
        Tells the workQueue component that a particular subscription,
        or set of subscriptions, is done.  Receives confirmation
        """

        doneList = []

        #In the future, this will talk to the workQueue
        #Right now it doesn't because I'm not sure how to do it
        doneList = subList

        return doneList

    def cleanWorkArea(self, doneList):
        """
        _cleanWorkArea_
        
        Upon workQueue realizing that a subscriptions is done, everything
        regarding those jobs is cleaned up.
        """

        myThread = threading.currentThread()
        #myThread.transaction.commit()

        for subscription in doneList:
            wmbsSubscription = Subscription(id = subscription["id"])
            wmbsSubscription.load()
            for job in wmbsSubscription.getJobs():
                wmbsJob = Job(id = job['id'])
                wmbsJob.load()
                self.cleanJobCache(wmbsJob.getCache())
            wmbsSubscription.deleteEverything()
        
        return

    def cleanJobCache(self, cacheDir):
        """
        _cleanJobCache_

        Clears out any files still sticking around in the jobCache, tars up the contents and sends them off
        """

        myThread = threading.currentThread()

        if not cacheDir or not os.path.isdir(cacheDir):
            logging.error("Could not find jobCacheDir %s" %(cacheDir))
            return

        if os.listdir(cacheDir) == []:
            #Directory is empty.
            logging.error("jobCacheDir %s empty" %(cacheDir))
            return

        #Otherwise we have something in there
        tarString = ["tar"]
        tarString.append("-cvf")
        tarString.append('%s/Job_%i.tar ' %(job["cache_dir"], job['id']))
        for file in os.listdir(job["cache_dir"]):
            tarString.append('%s' %(os.path.join(job["cache_dir"], file)))

        #Now we should have all the files together.  Tar them up
        pipe = Popen(tarString, stdout = PIPE, stderr = PIPE, shell = False)
        pipe.wait()

        #This should wait for the result, and then move the file
        #Then delete
        return

        
    
