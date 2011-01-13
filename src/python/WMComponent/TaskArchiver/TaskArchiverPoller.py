#!/usr/bin/env python
#pylint: disable-msg=W6501, W0142
# W6501: pass information to logging using string arguments
# W0142: Some people like ** magic
"""
The actual taskArchiver algorithm

Procedure:
a) Takes as input all finished subscriptions
      This is defined by the Subscriptions.GetFinishedSubscriptions DAO
b) Calls the WMBS.Subscription.DeleteEverything() method on them.

This should be a simple process.  Because of the long time between
the submission of subscriptions projected and the short time to run
this class, it should be run irregularly.
"""
__all__ = []

import os.path
import shutil
import threading
import logging
import traceback

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Fileset        import Fileset
from WMCore.DAOFactory          import DAOFactory
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.WMException         import WMException

from WMComponent.JobCreator.CreateWorkArea import getMasterName

class TaskArchiverPollerException(WMException):
    """
    _TaskArchiverPollerException_

    This is the class that serves as the customized
    Exception class for the TaskArchiverPoller

    As if you couldn't tell that already
    """

class TaskArchiverPoller(BaseWorkerThread):
    """
    Polls for Ended jobs

    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)

        self.config      = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir
        
        if getattr(self.config.TaskArchiver, "useWorkQueue", False) != False:
            wqp = self.config.TaskArchiver.WorkQueueParams
            self.workQueue = localQueue(**wqp)
        else:
            self.workQueue = None

        self.timeout = getattr(self.config.TaskArchiver, "timeOut", 0)
        return        
    

    def terminate(self, params):
        """
        _terminate_

        This function terminates the job after a final pass
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return


    

    def algorithm(self, parameters = None):
        """
	Performs the archiveJobs method, looking for each type of failure
	And deal with it as desired.
        """
        logging.debug("Running algorithm for finding finished subscriptions")
        try:
            self.archiveTasks()
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception, ex:
            myThread = threading.currentThread()
            msg = "Caught exception in TaskArchiver\n"
            msg += str(ex)
            if getattr(myThread, 'transaction', False) \
                   and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise TaskArchiverPollerException(msg)

        return


    def archiveTasks(self):
        """
        _archiveTasks_
        
        archiveTasks will handle the master task of looking for finished subscriptions,
        checking to see if they've finished, and then notifying the workQueue and
        finishing things up.
        """


        subList = self.findFinishedSubscriptions()
        if len(subList) == 0:
            return

        if self.workQueue != None:
            doneList = self.notifyWorkQueue(subList)
            self.killSubscriptions(doneList)
        else:
            self.killSubscriptions(subList)
            
        return

    def findFinishedSubscriptions(self):
        """
        _findFinishedSubscriptions_
        
        Figures out which one of the subscriptions is actually finished.
        """
        subList = []

        myThread = threading.currentThread()

        myThread.transaction.begin()

        subscriptionList = self.daoFactory(classname = "Subscriptions.GetFinishedSubscriptions")
        subscriptions    = subscriptionList.execute(timeOut = self.timeout)

        for subscription in subscriptions:
            wmbsSubscription = Subscription(id = subscription['id'])
            subList.append(wmbsSubscription)
            logging.info("Found subscription %i" %subscription['id'])

        myThread.transaction.commit()

        return subList


    def notifyWorkQueue(self, subList):
        """
        _notifyWorkQueue_
        
        Tells the workQueue component that a particular subscription,
        or set of subscriptions, is done.  Receives confirmation
        """
        subIDs = []
        
        for sub in subList:
            subIDs.append(sub['id'])        
        
        try:
            self.workQueue.doneWork(subIDs, id_type = "subscription_id")
            return subList
        except Exception, ex:
            logging.error("Error talking to workqueue: %s" % str(ex))
            logging.error("Tried to complete the following: %s" % subIDs)

        return []

    def killSubscriptions(self, doneList):
        """
        _killSubscriptions_
        
        Actually dump the subscriptions
        """

        for sub in doneList:
            logging.info("Deleting subscription %i" % sub['id'])
            try:
                sub.deleteEverything()
                workflow = sub['workflow']
                if not workflow.exists():
                    # Then we deleted the workflow
                    # Now we have to delete the task area.
                    workDir, taskDir = getMasterName(startDir = self.jobCacheDir,
                                                     workflow = workflow)
                    logging.error("About to delete work directory %s\n" % taskDir)
                    if os.path.isdir(taskDir):
                        # Remove the taskDir, because we're done
                        shutil.rmtree(taskDir)
            except Exception, ex:
                msg =  "Critical error while deleting subscription %i\n" % sub['id']
                msg += str(ex)
                logging.error(msg)
                raise TaskArchiverPollerException(msg)

        return



