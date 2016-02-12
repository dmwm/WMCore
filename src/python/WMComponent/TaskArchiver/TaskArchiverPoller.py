#!/usr/bin/env python
#pylint: disable=W0142
# W0142: Some people like ** magic
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
__all__ = []
import logging
import httplib
import threading
import traceback

from WMCore.DAOFactory                           import DAOFactory
from WMCore.WMException                          import WMException
from WMCore.WorkQueue.WorkQueue                  import localQueue
from WMCore.WorkQueue.WorkQueueExceptions        import WorkQueueNoMatchingElements
from WMCore.WorkerThreads.BaseWorkerThread       import BaseWorkerThread
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.ReqMgr.ReqMgr               import ReqMgr
from WMCore.Services.RequestDB.RequestDBWriter   import RequestDBWriter

from WMComponent.TaskArchiver.DataCache import DataCache 

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

    List of attributes

    requireCouch:  raise an exception on couch failure instead of ignoring
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
        
        self.dbsDaoFactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                     logger = myThread.logger, 
                                     dbinterface = myThread.dbi)

        self.config      = config
        self.jobCacheDir = self.config.JobCreator.jobCacheDir

        if getattr(self.config.TaskArchiver, "useWorkQueue", False) != False:
            # Get workqueue setup from config unless overridden
            if hasattr(self.config.TaskArchiver, 'WorkQueueParams'):
                self.workQueue = localQueue(**self.config.TaskArchiver.WorkQueueParams)
            else:
                from WMCore.WorkQueue.WorkQueueUtils import queueFromConfig
                self.workQueue = queueFromConfig(self.config)
        else:
            self.workQueue = None

        self.timeout           = getattr(self.config.TaskArchiver, "timeOut", None)
        self.useReqMgrForCompletionCheck   = getattr(self.config.TaskArchiver, 'useReqMgrForCompletionCheck', True)
        
        if not self.useReqMgrForCompletionCheck:
            #sets the local monitor summary couch db
            self.requestLocalCouchDB = RequestDBWriter(self.config.AnalyticsDataCollector.localT0RequestDBURL, 
                                                   couchapp = self.config.AnalyticsDataCollector.RequestCouchApp)
            self.centralCouchDBWriter = self.requestLocalCouchDB;
        else:
            self.centralCouchDBWriter = RequestDBWriter(self.config.AnalyticsDataCollector.centralRequestDBURL)
            
            self.reqmgr2Svc = ReqMgr(self.config.TaskArchiver.ReqMgr2ServiceURL)
            #TODO: remove this when reqmgr2 replace reqmgr completely (reqmgr2Only)
            self.reqmgrSvc = RequestManager({'endpoint': self.config.TaskArchiver.ReqMgrServiceURL})

        #Load the cleanout state ID and save it
        stateIDDAO = self.daoFactory(classname = "Jobs.GetStateID")
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

    def algorithm(self, parameters = None):
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

        #Get the subscriptions that are now finished and mark them as such
        logging.info("Polling for finished subscriptions")
        finishedSubscriptions = self.daoFactory(classname = "Subscriptions.MarkNewFinishedSubscriptions")
        finishedSubscriptions.execute(self.stateID, timeOut = self.timeout)
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
        
        finishedWorkflowsDAO = self.daoFactory(classname = "Workflow.GetFinishedWorkflows")
        finishedwfs = finishedWorkflowsDAO.execute()
        finishedLogCollectAndCleanUpwfs = finishedWorkflowsDAO.execute(onlySecondary=True)
        finishedwfsWithLogCollectAndCleanUp = {}
        for wf in finishedLogCollectAndCleanUpwfs:
            if wf in finishedwfs:
                finishedwfsWithLogCollectAndCleanUp[wf] = finishedwfs[wf]
        return (finishedwfs, finishedwfsWithLogCollectAndCleanUp)
        
    def completeTasks(self, finishedwfs):
        """
        _completeTasks_

        This method will call several auxiliary methods to do the following:
        
        1. Notify the WorkQueue about finished subscriptions
        2. update dbsbuffer_workflow table with finished subscription
        """


        #Only delete those where the upload and notification succeeded
        logging.info("Found %d candidate workflows for completing: %s" % (len(finishedwfs),finishedwfs.keys()))
        # update the completed flag in dbsbuffer_workflow table so blocks can be closed
        # create updateDBSBufferWorkflowComplete DAO
        if len(finishedwfs) == 0:
            return
        
        completedWorkflowsDAO = self.dbsDaoFactory(classname = "UpdateWorkflowsToCompleted")
        
        centralCouchAlive = True
        try:
            #TODO: need to enable when reqmgr2 -wmstats is ready
            #abortedWorkflows = self.reqmgrCouchDBWriter.getRequestByStatus(["aborted"], format = "dict");
            abortedWorkflows = self.centralCouchDBWriter.getRequestByStatus(["aborted"])
            logging.info("There are %d requests in 'aborted' status in central couch." % len(abortedWorkflows))
            forceCompleteWorkflows = self.centralCouchDBWriter.getRequestByStatus(["force-complete"])
            logging.info("List of 'force-complete' workflows in central couch: %s" % forceCompleteWorkflows)
            
        except Exception as ex:
            centralCouchAlive = False
            logging.error("we will try again when remote couch server comes back\n%s" % str(ex))
        
        if centralCouchAlive:
            for workflow in finishedwfs:
                try:
                    #Notify the WorkQueue, if there is one
                    if self.workQueue != None:
                        subList = []
                        logging.info("Marking subscriptions as Done ...")
                        for l in finishedwfs[workflow]["workflows"].values():
                            subList.extend(l)
                        self.notifyWorkQueue(subList)
                    
                    #Now we know the workflow as a whole is gone, we can delete the information from couch
                    if not self.useReqMgrForCompletionCheck:
                        self.requestLocalCouchDB.updateRequestStatus(workflow, "completed")
                        logging.info("status updated to completed %s" % workflow)
    
                    if workflow in abortedWorkflows:
                        #TODO: remove when reqmgr2-wmstats deployed
                        newState = "aborted-completed"
                    elif workflow in forceCompleteWorkflows:
                        newState = "completed"
                    else:
                        newState = None
                        
                    if newState != None:
                        # update reqmgr workload document only request mgr is installed
                        if not self.useReqMgrForCompletionCheck:
                            # commented out untill all the agent is updated so every request have new state
                            # TODO: agent should be able to write reqmgr db diretly add the right group in
                            # reqmgr
                            self.requestLocalCouchDB.updateRequestStatus(workflow, newState)
                        else:
                            try:
                                #TODO: try reqmgr1 call if it fails (reqmgr2Only - remove this line when reqmgr is replaced)
                                logging.info("Updating status to '%s' in both oracle and couchdb ..." % newState)
                                self.reqmgrSvc.updateRequestStatus(workflow, newState)
                                #And replace with this - remove all the excption
                                #self.reqmgr2Svc.updateRequestStatus(workflow, newState)
                            except httplib.HTTPException as ex:
                                # If we get an HTTPException of 404 means reqmgr2 request
                                if ex.status == 404:
                                    # try reqmgr2 call
                                    msg = "%s : reqmgr2 request: %s" % (workflow, str(ex))
                                    logging.warning(msg)
                                    self.reqmgr2Svc.updateRequestStatus(workflow, newState)
                                else:
                                    msg = "%s : fail to update status %s  with HTTP error: %s" % (workflow, newState, str(ex))
                                    logging.error(msg)
                                    raise ex
                            
                        logging.info("status updated to '%s' : %s" % (newState, workflow))
                    
                    completedWorkflowsDAO.execute([workflow])
        
                except TaskArchiverPollerException as ex:

                    #Something didn't go well when notifying the workqueue, abort!!!
                    logging.error("Something bad happened while archiving tasks.")
                    logging.error(str(ex))
                    self.sendAlert(1, msg = str(ex))
                    continue
                except Exception as ex:
                    #Something didn't go well on couch, abort!!!
                    msg = "Problem while archiving tasks for workflow %s\n" % workflow
                    msg += "Exception message: %s" % str(ex)
                    msg += "\nTraceback: %s" % traceback.format_exc()
                    logging.error(msg)
                    self.sendAlert(3, msg = msg)
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
                self.workQueue.doneWork(SubscriptionId = sub)
            except WorkQueueNoMatchingElements:
                #Subscription wasn't known to WorkQueue, feel free to clean up
                logging.info("Local WorkQueue knows nothing about this subscription: %s" % sub)
                pass
            except Exception as ex:
                msg = "Error talking to workqueue: %s\n" % str(ex)
                msg += "Tried to complete the following: %s\n" % sub
                raise TaskArchiverPollerException(msg)

        return

