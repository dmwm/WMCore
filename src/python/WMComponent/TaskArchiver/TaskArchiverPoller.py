#!/usr/bin/env python
"""
The actual taskArchiver algorithm

Procedure:
a) Find and marks a finished all newly finished subscriptions
      This is defined by the Subscriptions.MarkNewFinishedSubscriptions DAO
b) Look for finished workflows as defined in the Workflow.GetFinishedWorkflows DAO
c) Upload couch summary information
d) Call WMBS.Subscription.deleteEverything() on all the associated subscriptions
e) Delete couch information and working directories

This should be a simple process.  Because of the long time between
the submission of subscriptions projected and the short time to run
this class, it should be run irregularly.


Config options
histogramKeys: Allows you to report values in histogram form in the
  workloadSummary - i.e., as a list of bins
histogramBins: Bin size for all histograms
histogramLimit: Limit in terms of number of standard deviations from the
  average at which you cut the histogram off.  All points outside of that
  go into overflow and underflow.
"""
from builtins import str as newstr, bytes
from future.utils import viewvalues

import logging
import threading
import traceback

from Utils.Timers import timeFunction
from WMComponent.TaskArchiver.DataCache import DataCache
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter
from WMCore.WMException import WMException
from WMCore.WorkQueue.WorkQueue import localQueue
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoMatchingElements
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread


class TaskArchiverPollerException(WMException):
    """
    _TaskArchiverPollerException_

    This is the class that serves as the customized
    Exception class for the TaskArchiverPoller

    As if you couldn't tell that already
    """
    pass


class TaskArchiverPoller(BaseWorkerThread):
    """
    Polls for Ended jobs

    List of attributes

    requireCouch:  raise an exception on couch failure instead of ignoring
    """

    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)

        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.dbsDaoFactory = DAOFactory(package="WMComponent.DBS3Buffer",
                                        logger=myThread.logger,
                                        dbinterface=myThread.dbi)

        self.config = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir

        if getattr(self.config.TaskArchiver, "useWorkQueue", False):
            # Get workqueue setup from config unless overridden
            if hasattr(self.config.TaskArchiver, 'WorkQueueParams'):
                self.workQueue = localQueue(**self.config.TaskArchiver.WorkQueueParams)
            else:
                from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
                self.workQueue = queueFromConfig(self.config)
        else:
            self.workQueue = None

        self.timeout = getattr(self.config.TaskArchiver, "timeOut", None)
        self.useReqMgrForCompletionCheck = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)

        if not self.useReqMgrForCompletionCheck:
            # sets the local monitor summary couch db
            self.tier0CompletedState = "completed"
            self.requestLocalCouchDB = RequestDBWriter(self.config.AnalyticsDataCollector.localT0RequestDBURL,
                                                       couchapp=self.config.AnalyticsDataCollector.RequestCouchApp)
            self.centralCouchDBWriter = self.requestLocalCouchDB
        else:
            self.tier0CompletedState = None
            self.centralCouchDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.centralRequestDBURL)

            self.reqmgr2Svc = ReqMgr(self.config.General.ReqMgr2ServiceURL)

        # Load the cleanout state ID and save it
        stateIDDAO = self.daoFactory(classname="Jobs.GetStateID")
        self.stateID = stateIDDAO.execute("cleanout")

        return

    def terminate(self, params):
        """
        _terminate_

        This function terminates the job after a final pass
        """
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return

    @timeFunction
    def algorithm(self, parameters=None):
        """
        _algorithm_

        Executes the two main methods of the poller:
        1. findAndMarkFinishedSubscriptions
        2. completeTasks
        Final result is that finished workflows get their summary built and uploaded to couch,
        and all traces of them are removed from the agent WMBS and couch (this last one on demand).
        """
        try:
            self.findAndMarkFinishedSubscriptions()
            (finishedwfs, finishedwfsWithLogCollectAndCleanUp) = self.getFinishedWorkflows()
            # set the data cache which can be used other thread (no ther thread should set the data cache)
            DataCache.setFinishedWorkflows(finishedwfsWithLogCollectAndCleanUp)
            self.completeTasks(finishedwfs)
        except WMException:
            myThread = threading.currentThread()
            if getattr(myThread, 'transaction', False) \
                    and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise
        except Exception as ex:
            myThread = threading.currentThread()
            msg = "Caught exception in TaskArchiver\n"
            msg += str(ex)
            if getattr(myThread, 'transaction', False) \
                    and getattr(myThread.transaction, 'transaction', False):
                myThread.transaction.rollback()
            raise TaskArchiverPollerException(msg)

        return

    def findAndMarkFinishedSubscriptions(self):
        """
        _findAndMarkFinishedSubscriptions_

        Find new finished subscriptions and mark as finished in WMBS.
        """
        myThread = threading.currentThread()

        myThread.transaction.begin()

        # Get the subscriptions that are now finished and mark them as such
        logging.info("Polling for finished subscriptions")
        finishedSubscriptions = self.daoFactory(classname="Subscriptions.MarkNewFinishedSubscriptions")
        finishedSubscriptions.execute(self.stateID, timeOut=self.timeout)
        logging.info("Finished subscriptions updated")

        myThread.transaction.commit()

        return

    def getFinishedWorkflows(self):
        """
        1. Get finished workflows (a finished workflow is defined in Workflow.GetFinishedWorkflows)
        2. Get finished workflows with logCollect and Cleanup only.
        3. combined those and make return
           finishedwfs - without LogCollect and CleanUp task
           finishedwfsWithLogCollectAndCleanUp - including LogCollect and CleanUp task
        """

        finishedWorkflowsDAO = self.daoFactory(classname="Workflow.GetFinishedWorkflows")
        finishedwfs = finishedWorkflowsDAO.execute()
        finishedLogCollectAndCleanUpwfs = finishedWorkflowsDAO.execute(onlySecondary=True)
        finishedwfsWithLogCollectAndCleanUp = {}
        for wf in finishedLogCollectAndCleanUpwfs:
            if wf in finishedwfs:
                finishedwfsWithLogCollectAndCleanUp[wf] = finishedwfs[wf]
        return (finishedwfs, finishedwfsWithLogCollectAndCleanUp)

    def killCondorJobsByWFStatus(self, statusList):
        if isinstance(statusList, (newstr, bytes)):
            statusList = [statusList]
        reqNames = self.centralCouchDBWriter.getRequestByStatus(statusList)
        logging.info("There are %d requests in %s status in central couch.", len(reqNames), statusList)
        if self.workQueue is not None:
            self.workQueue.killWMBSWorkflows(reqNames)
        return reqNames

    def completeTasks(self, finishedwfs):
        """
        _completeTasks_

        This method will call several auxiliary methods to do the following:

        1. Notify the WorkQueue about finished subscriptions
        2. mark workflow as completed in the dbsbuffer_workflow table
        """
        if not finishedwfs:
            return

        logging.info("Found %d candidate workflows for completing:", len(finishedwfs))
        completedWorkflowsDAO = self.dbsDaoFactory(classname="UpdateWorkflowsToCompleted")

        centralCouchAlive = True
        try:
            self.killCondorJobsByWFStatus(["force-complete", "aborted"])
        except Exception as ex:
            centralCouchAlive = False
            logging.error("we will try again when remote couch server comes back\n%s", str(ex))

        if centralCouchAlive:
            logging.info("Marking subscriptions as Done ...")
            for workflow in finishedwfs:
                try:
                    # Notify the WorkQueue, if there is one
                    if self.workQueue is not None:
                        subList = []
                        for l in viewvalues(finishedwfs[workflow]["workflows"]):
                            subList.extend(l)
                        self.notifyWorkQueue(subList)

                    # Tier-0 case, the agent has to mark it completed
                    if not self.useReqMgrForCompletionCheck:
                        resp = self.requestLocalCouchDB.updateRequestStatus(workflow, self.tier0CompletedState)
                        logging.info("Workflow %s updated to status '%s'. Response: %s",
                                     workflow, self.tier0CompletedState, resp)

                    completedWorkflowsDAO.execute([workflow])

                except TaskArchiverPollerException as ex:
                    # Something didn't go well when notifying the workqueue, abort!!!
                    logging.error("Something bad happened while archiving tasks.")
                    logging.error(str(ex))
                    continue
                except Exception as ex:
                    # Something didn't go well on couch, abort!!!
                    msg = "Problem while archiving tasks for workflow %s\n" % workflow
                    msg += "Exception message: %s" % str(ex)
                    msg += "\nTraceback: %s" % traceback.format_exc()
                    logging.error(msg)
                    continue
        return

    def notifyWorkQueue(self, subList):
        """
        _notifyWorkQueue_

        Tells the workQueue component that a particular subscription,
        or set of subscriptions, is done.  Receives confirmation
        """

        for sub in subList:
            try:
                self.workQueue.doneWork(SubscriptionId=sub)
            except WorkQueueNoMatchingElements:
                # Subscription wasn't known to WorkQueue, feel free to clean up
                logging.debug("Local WorkQueue knows nothing about this subscription: %s", sub)
            except Exception as ex:
                msg = "Error talking to workqueue: %s\n" % str(ex)
                msg += "Tried to complete the following: %s\n" % sub
                raise TaskArchiverPollerException(msg)

        return
