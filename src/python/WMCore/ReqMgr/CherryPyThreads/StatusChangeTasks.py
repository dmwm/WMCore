"""
Created on May 19, 2015
"""
from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.DataStructs.RequestStatus import AUTO_TRANSITION
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


# FIXME: remove this function once MS Transferor is solid and in production
def moveTransferorStatus(reqmgrSvc, logger):
    """
    Function to temporarily make the status transition supposed
    to happen in the MS Transferor module.
    Once that's fully functional and in production, we can completely
    remove this function.
    """
    logger.info("Advancing MicroServices statuses")
    thisTransition = {"assigned": "staging",
                      "staging": "staged"}
    # Let's try to keep these requests in this status for a bit longer
    # and give MS Transferor a chance to find them and run its logic.
    # That's why starting with the inverse order
    for status in ["staging", "assigned"]:
        requests = reqmgrSvc.getRequestByStatus([status], detail=False)
        newStatus = thisTransition[status]
        for wf in requests:
            reqmgrSvc.updateRequestStatus(wf, newStatus)
            logger.info("%s in %s moved to %s", wf, status, newStatus)
        logger.info("%d requests moved from: %s to: %s", len(requests), status, newStatus)
    return


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
            elif stateFromGQ == "failed" and status == "staged":
                count += 1
                reqmgrSvc.updateRequestStatus(wf, nextStatus[0])
                logger.info("%s in %s moved to %s", wf, status, nextStatus[0])
                continue
            elif stateFromGQ == "failed" and status == "acquired":
                count += 1
                reqmgrSvc.updateRequestStatus(wf, stateFromGQ)
                logger.info("%s in %s moved to %s", wf, status, stateFromGQ)
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
                logger.info("%s in %s moved to %s", wf, status, "aborted-completed")
            else:
                for j in range(i + 1):
                    count += 1
                    reqmgrSvc.updateRequestStatus(wf, nextStatus[j])
                    logger.info("%s in %s moved to %s", wf, status, nextStatus[j])
        logger.info("%s requests moved to new state from %s", count, status)
    return


def moveToCompletedForNoWQJobs(reqmgrSvc, wfStatusDict, logger):
    """
    Handle the case when request is aborted/rejected before elements are created in GQ
    """

    statusTransition = {"aborted": ["aborted-completed"]}

    for status, nextStatusList in statusTransition.items():
        requests = reqmgrSvc.getRequestByStatus([status], detail=False)
        count = 0
        for wf in requests:
            # check whether wq elements exists for given request
            # if not, it means
            if wf not in wfStatusDict:
                for nextStatus in nextStatusList:
                    reqmgrSvc.updateRequestStatus(wf, nextStatus)
                    count += 1
        logger.info("Total aborted-completed: %d", count)

    return


class StatusChangeTasks(CherryPyPeriodicTask):
    def __init__(self, rest, config):
        super(StatusChangeTasks, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        Define which function to periodically run
        """
        self.concurrentTasks = [{'func': self.advanceStatus, 'duration': config.checkStatusDuration}]

    def advanceStatus(self, config):
        """
        Advance the request status based on the global workqueue elements status
        """
        reqmgrSvc = ReqMgr(config.reqmgr2_url, logger=self.logger)
        gqService = WorkQueue(config.workqueue_url)

        self.logger.info("Getting GQ data for status check")
        wfStatusDict = gqService.getWorkflowStatusFromWQE()

        self.logger.info("Advancing statuses")
        if getattr(config, "enableMSStatusTransition", False):
            moveTransferorStatus(reqmgrSvc, self.logger)
        moveForwardStatus(reqmgrSvc, wfStatusDict, self.logger)
        moveToCompletedForNoWQJobs(reqmgrSvc, wfStatusDict, self.logger)

        self.logger.info("Done advancing status")

        return
