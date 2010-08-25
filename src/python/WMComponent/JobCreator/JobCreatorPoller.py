#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The JobCreator Poller for the JSM
"""
__all__ = []
__revision__ = "$Id: JobCreatorPoller.py,v 1.18 2010/04/29 20:14:51 mnorman Exp $"
__version__  = "$Revision: 1.18 $"

import threading
import logging
import os
import os.path
import traceback
#import time
#import cProfile, pstats


from WMCore.WorkerThreads.BaseWorkerThread  import BaseWorkerThread
from WMCore.DAOFactory                      import DAOFactory

from WMCore.WMSpec.Makers.Interface.CreateWorkArea      import CreateWorkArea
from WMCore.ProcessPool.ProcessPool                     import ProcessPool


class JobCreatorPoller(BaseWorkerThread):

    """
    Poller that does the work of job creation.
    Polls active subscriptions, asks for more work, and checks with local sites.

    """


    def __init__(self, config):
        """
        init jobCreator
        """

        myThread = threading.currentThread()

        #DAO factory for WMBS objects
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = logging,
                                     dbinterface = myThread.dbi)
        
        #information
        self.config = config


        #Variables
        self.jobCacheDir    = config.JobCreator.jobCacheDir
        self.defaultJobType = config.JobCreator.defaultJobType

        
        BaseWorkerThread.__init__(self)

        #self.jobMaker = JobMaker(self.config)
        self.createWorkArea  = CreateWorkArea()

        configDict = {'jobCacheDir': self.config.JobCreator.jobCacheDir, 
                      'defaultJobType': config.JobCreator.defaultJobType, 
                      'couchURL': self.config.JobStateMachine.couchurl, 
                      'defaultRetries': self.config.JobStateMachine.default_retries,
                      'couchDBName': self.config.JobStateMachine.couchDBName}

        self.processPool = ProcessPool("JobCreator.JobCreatorWorker",
                                       totalSlaves = self.config.JobCreator.workerThreads,
                                       componentDir = self.config.JobCreator.componentDir,
                                       config = self.config,
                                       slaveInit = configDict)



        #Testing
        self.timing = {'pollSites': 0, 'pollSubscriptions': 0, 'pollJobs': 0,
                       'askWorkQueue': 0, 'pollSubList': 0, 'pollSubJG': 0, 
                       'pollSubSplit': 0, 'baggage': 0, 'createWorkArea': 0}

        return

    def check(self):
        """
        Initial sanity checks on necessary environment factors

        """

        if not os.path.isdir(self.jobCacheDir):
            if not os.path.exists(self.jobCacheDir):
                os.makedirs(self.jobCacheDir)
            else:
                msg = "Assigned a non-existant cache directory %s.  Failing!" \
                      % (self.jobCacheDir)
                raise Exception (msg)


    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        try:
            self.runJobCreator()
        except Exception, ex:
            #myThread.transaction.rollback()
            msg = "Failed to execute JobCreator \n%s\n" % (ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            raise Exception(msg)

        #print self.timing
        #print "Job took %f seconds" %(time.clock()-startTime)

        

    def runJobCreator(self):
        """
        Highest level manager for job creation

        """

        #This should do three tasks:
        #Poll current subscriptions and create jobs.
        
        self.check()

        self.pollSubscriptions()
    
        return




    def pollSubscriptions(self):
        """
        Poller for looking in all active subscriptions for jobs that need to be made.

        """
        logging.info("Polling Subscription")
        myThread = threading.currentThread()

        #First, get list of Subscriptions

        subscriptionList = self.daoFactory(classname = "Subscriptions.ListIncomplete")
        subscriptions    = subscriptionList.execute()

        myThread.transaction.commit()


        #Now go through each one looking for jobs
        listOfWork = []
        for subscription in subscriptions:


            #Create a dictionary
            tmpDict = {'subscription': subscription}
            listOfWork.append(tmpDict)
            
        if listOfWork != []:
            # Only enqueue if we have work to do!
            self.processPool.enqueue(listOfWork)
            self.processPool.dequeue(totalItems = len(listOfWork))
        
        logging.info("Number of subscription enqueued is %i" % len(listOfWork))

        return


    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)





