"""
Created on May 19, 2015
"""
from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.DataStructs.RequestStatus import AUTO_TRANSITION
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


def moveForwardStatus(reqmgrSvc, wfStatusDict, logger):
    
    for status, nextStatus in AUTO_TRANSITION.iteritems():
        count = 0
        requests = reqmgrSvc.getRequestByStatus([status], detail=False)
        for wf in requests:
            stateFromGQ = wfStatusDict.get(wf, None)
            if stateFromGQ is None:
                continue
            elif stateFromGQ == status:
                continue
            elif stateFromGQ == "failed" and status == "assigned":
                count += 1
                reqmgrSvc.updateRequestStatus(wf, stateFromGQ)
                logger.debug("%s in %s moved to %s", wf, status, stateFromGQ)
                continue

            try:
                i = nextStatus.index(stateFromGQ)
            except ValueError:
                # No state change needed
                continue
            # special case for aborted workflow - aborted-completed instead of completed
            if status == "aborted" and i == 0:
                count += 1
                reqmgrSvc.updateRequestStatus(wf, "aborted-completed")
                logger.debug("%s in %s moved to %s", wf, status, "aborted-completed")
            else:
                for j in range(i + 1):
                    count += 1
                    reqmgrSvc.updateRequestStatus(wf, nextStatus[j])
                    logger.debug("%s in %s moved to %s", wf, status, nextStatus[j])
        logger.info("%s requests moved to new state from %s", count, status)
    return


def moveToArchivedForNoJobs(reqmgrSvc, wfStatusDict, logger):
    """
    Handle the case when request is aborted/rejected before elements are created in GQ
    """

    statusTransition = {"aborted": ["aborted-completed", "aborted-archived"],
                        "aborted-completed": ["aborted-archived"],
                        "rejected": ["rejected-archived"]}

    for status, nextStatusList in statusTransition.items():
        requests = reqmgrSvc.getRequestByStatus([status])
        count = 0
        for wf in requests:
            # check whether wq elements exists for given request
            # if not, it means 
            if wf not in wfStatusDict:
                for nextStatus in nextStatusList:
                    reqmgrSvc.updateRequestStatus(wf, nextStatus)
                    count += 1
        # convert to start status for the logging purpose
        initStatus = "aborted" if status == "aborted-completed" else status
        logger.info("Total %s-archived: %d", initStatus, count)

    return


class StatusChangeTasks(CherryPyPeriodicTask):
    def __init__(self, rest, config):
        super(StatusChangeTasks, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.advanceStatus, 'duration': config.checkStatusDuration}]

    def advanceStatus(self, config):
        """
        gather active data statistics
        """

        reqmgrSvc = ReqMgr(config.reqmgr2_url)
        gqService = WorkQueue(config.workqueue_url)
        
        self.logger.info("Getting GQ data for status check")
        wfStatusDict = gqService.getWorkflowStatusFromWQE()
        
        self.logger.info("Advancing status")
        moveForwardStatus(reqmgrSvc, wfStatusDict, self.logger)
        moveToArchivedForNoJobs(reqmgrSvc, wfStatusDict, self.logger)

        return
