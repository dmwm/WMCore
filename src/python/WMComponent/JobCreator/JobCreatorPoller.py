#!/usr/bin/env python
#pylint: disable-msg=W0613, W6501
"""
The JobCreator Poller for the JSM
"""
__all__ = []



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
        self.jobCacheDir        = config.JobCreator.jobCacheDir
        self.defaultJobType     = config.JobCreator.defaultJobType
        self.count              = 0
        self.processPoolRestart = getattr(config.JobCreator, 'processPoolRestart', 20)
        self.restartProcessPool = getattr(config.JobCreator, 'restartProcessPool', False)

        
        BaseWorkerThread.__init__(self)

        #self.jobMaker = JobMaker(self.config)
        self.createWorkArea  = CreateWorkArea()

        configDict = {'jobCacheDir': self.config.JobCreator.jobCacheDir, 
                      'defaultJobType': config.JobCreator.defaultJobType, 
                      'couchURL': self.config.JobStateMachine.couchurl, 
                      'defaultRetries': self.config.JobStateMachine.default_retries,
                      'couchDBName': self.config.JobStateMachine.couchDBName,
                      'fileLoadLimit': getattr(self.config.JobCreator, 'fileLoadLimit', 500)}

        self.processPool = ProcessPool("JobCreator.JobCreatorWorker",
                                       totalSlaves = self.config.JobCreator.workerThreads,
                                       componentDir = self.config.JobCreator.componentDir,
                                       config = self.config,
                                       slaveInit = configDict)



        self.check()

        return

    def check(self):
        """
        Initial sanity checks on necessary environment factors

        """

        if not os.path.isdir(self.jobCacheDir):
            if not os.path.exists(self.jobCacheDir):
                os.makedirs(self.jobCacheDir)
            else:
                msg = "Assigned a pre-existant cache object %s.  Failing!" \
                      % (self.jobCacheDir)
                raise Exception (msg)


    def algorithm(self, parameters = None):
        """
        Actually runs the code
        """
        logging.debug("Running JSM.JobCreator")
        try:
            self.pollSubscriptions()
        except Exception, ex:
            msg = "Failed to execute JobCreator \n%s\n" % (ex)
            msg += str(traceback.format_exc())
            msg += "\n\n"
            raise Exception(msg)

        




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


        # Create a list of subscriptions to send to
        # JobCreatorWorkers
        listOfWork = []
        for subscription in subscriptions:
            listOfWork.append({'subscription': subscription})

        logging.debug("Enqueuing the following work: %s" % listOfWork)
            
        if listOfWork != []:
            # Only enqueue if we have work to do!
            logging.debug("About to enqueue %i items" % (len(listOfWork)))
            self.processPool.enqueue(listOfWork)
            self.processPool.dequeue(totalItems = len(listOfWork))
            logging.debug("Successfully dequeued work")
            self.count += 1


        # If done, and you've done this several times, restart
        if self.count >= self.processPoolRestart and self.restartProcessPool:
            logging.debug("Restarting processPool")
            self.processPool.restart()
            self.count = 0

    
        return


    def terminate(self, params):
        """
        _terminate_
        
        Kill the code after one final pass when called by the master thread.
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)





