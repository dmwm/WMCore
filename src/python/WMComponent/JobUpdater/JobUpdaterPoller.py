"""
__JobUpdaterPoller__

Poller module for the JobUpdater, takes care of the
actual updating work.

Created on Apr 16, 2013

@author: dballest
"""

import logging
import threading
from Utils.Timers import timeFunction
from WMCore.BossAir.BossAirAPI import BossAirAPI
from WMCore.DAOFactory import DAOFactory
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMException import WMException
from WMCore.Database.CMSCouch import CouchConflictError, CouchError
from WMCore.Database.CouchUtils import CouchConnectionError


class JobUpdaterException(WMException):
    """
    _JobUpdaterException_

    A job updater exception-handling class for the JobUpdaterPoller
    """
    pass


class JobUpdaterPoller(BaseWorkerThread):
    """
    _JobUpdaterPoller_

    Poller class for the JobUpdater
    """

    def __init__(self, config):
        """
        __init__
        """
        BaseWorkerThread.__init__(self)
        self.config = config

        self.bossAir = BossAirAPI(config=self.config)
        self.reqmgr2 = ReqMgr(self.config.General.ReqMgr2ServiceURL)
        self.workqueue = WorkQueue(self.config.WorkQueueManager.couchurl,
                                   self.config.WorkQueueManager.dbname)

        myThread = threading.currentThread()

        self.daoFactory = DAOFactory(package="WMCore.WMBS",
                                     logger=myThread.logger,
                                     dbinterface=myThread.dbi)

        self.listWorkflowsDAO = self.daoFactory(classname="Workflow.ListForJobUpdater")
        self.updateWorkflowPrioDAO = self.daoFactory(classname="Workflow.UpdatePriority")
        self.executingJobsDAO = self.daoFactory(classname="Jobs.GetNumberOfJobsForWorkflowStatus")

    def setup(self, parameters=None):
        """
        _setup_
        """
        pass

    def terminate(self, parameters=None):
        """
        _terminate_

        Terminate gracefully.
        """
        pass

    @timeFunction
    def algorithm(self, parameters=None):
        """
        _algorithm_
        """
        try:
            logging.info("Synchronizing priorities with ReqMgr...")
            self.synchronizeJobPriority()
            logging.info("Priorities were synchronized, wait until the next cycle")
        except CouchConnectionError as ex:
            msg = "Caught CouchConnectionError exception in JobUpdater\n"
            msg += "transactions postponed until the next polling cycle\n"
            msg += str(ex)
            logging.exception(msg)
        except CouchConflictError as ex:
            msg = "Caught CouchConflictError exception in JobUpdater\n"
            msg += "transactions postponed until the next polling cycle\n"
            msg += str(ex)
            logging.exception(msg)
        except CouchError as ex:
            if ex.status is None and ex.reason is None:
                msg = "Couch Server error occured. Mostly due to time out\n"
                msg += str(ex)
                logging.exception(msg)
            else:
                raise JobUpdaterException(str(ex))
        except Exception as ex:
            errorStr = str(ex)
            if 'Connection refused' in errorStr or "timed out" in errorStr:
                logging.warning("Failed to sync priorities. Trying in the next cycle")
            else:
                msg = "Caught unexpected exception in JobUpdater: %s\n" % errorStr
                logging.exception(msg)
                raise JobUpdaterException(msg)

    def getWorkflowPrio(self, wflowName):
        """
        Given a request name, it fetches data from ReqMgr2 and returns
        the request priority
        :param wflowName: string with the request name
        :return: integer with the request priority
        """
        result = None
        try:
            result = self.reqmgr2.getRequestByNames(wflowName)[0]
            result = int(result[wflowName]['RequestPriority'])
        except Exception as ex:
            logging.error("Couldn't retrieve the priority of request %s", wflowName)
            logging.error("Error: %s", str(ex))
        return result

    def synchronizeJobPriority(self):
        """
        _synchronizeJobPriority_

        Check WMBS and WorkQueue for active workflows and compare with the
        ReqMgr for priority changes. If a priority change occurs
        then update the job priority in the batch system and
        the elements in the local queue that have not been injected yet.
        """
        # Update the priority of workflows that are not in WMBS and just in local queue
        priorityCache = {}
        workflowsToUpdate = {}
        # this call returns workflows that only have Available elements in local queue
        workflowsToCheck = self.workqueue.getAvailableWorkflows()
        for workflow, workqueuePrio in workflowsToCheck:
            if workflow not in priorityCache:
                requestPrio = self.getWorkflowPrio(workflow)
                if requestPrio is None:
                    continue

                priorityCache[workflow] = requestPrio
                if workqueuePrio != priorityCache[workflow]:
                    logging.info("Updating workqueue elements priority for request: %s", workflow)
                    self.workqueue.updatePriority(workflow, priorityCache[workflow])

        # Check the workflows in WMBS
        workflowsToUpdateWMBS = {}
        workflowsToCheck = self.listWorkflowsDAO.execute()
        # this loop contains multiple entries for the same workflow
        for workflowEntry in workflowsToCheck:
            workflow = workflowEntry['name']
            wmbsPrio = int(workflowEntry['workflow_priority'])
            if workflow not in priorityCache:
                requestPrio = self.getWorkflowPrio(workflow)
                if requestPrio is None:
                    continue

                priorityCache[workflow] = requestPrio
                if wmbsPrio != priorityCache[workflow]:
                    logging.info("Updating workqueue elements priority for request: %s", workflow)
                    self.workqueue.updatePriority(workflow, priorityCache[workflow])

                    # if there are jobs in wmbs executing state, update their prio in condor
                    if self.executingJobsDAO.execute(workflow) > 0:
                        logging.info("Updating condor jobs priority for request: %s", workflow)
                        self.bossAir.updateJobInformation(workflow,
                                                          requestPriority=priorityCache[workflow])
                    workflowsToUpdateWMBS[workflow] = priorityCache[workflow]

        if workflowsToUpdateWMBS:
            logging.info("Updating WMBS workflow priority for %d workflows.", len(workflowsToUpdateWMBS))
            self.updateWorkflowPrioDAO.execute(workflowsToUpdateWMBS)
