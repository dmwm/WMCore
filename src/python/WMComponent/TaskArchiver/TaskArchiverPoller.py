#!/usr/bin/env python
"""
The actual taskArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: TaskArchiverPoller.py,v 1.7 2010/05/20 20:59:22 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

import threading
import logging
import os.path
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMBS.Subscription   import Subscription
from WMCore.WMBS.Job            import Job
from WMCore.WMBS.Fileset        import Fileset
from WMCore.DAOFactory          import DAOFactory
from WMCore.WorkQueue.WorkQueue import localQueue

class TaskArchiverPoller(BaseWorkerThread):
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
        
        self.daoFactory = DAOFactory(package = "WMCore.WMBS",
                                     logger = myThread.logger,
                                     dbinterface = myThread.dbi)
        if getattr(self.config.TaskArchiver, "useWorkQueue", False) != False:
            self.workQueue = localQueue(**self.config.TaskArchiver.WorkQueueParams)
        else:
            self.workQueue = None
    
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
        try:
            self.archiveTasks()
        except:
            raise

        return


    def archiveTasks(self):
        """
        _archiveTasks_
        
        archiveTaks will handle the master task of looking for finished subscriptions,
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
            
        #self.cleanWorkArea(doneList)
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
        subscriptions    = subscriptionList.execute()

        for subscription in subscriptions:
            wmbsSubscription = Subscription(id = subscription['id'])
            subList.append(wmbsSubscription)

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
        for subscription in doneList:
            subscription.deleteEverything()

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
        return
