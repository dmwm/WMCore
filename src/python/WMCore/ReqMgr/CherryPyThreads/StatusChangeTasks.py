"""
Created on May 19, 2015
"""

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.DataStructs.RequestStatus import AUTO_TRANSITION, CANCEL_AUTO_TRANSITION
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr


def moveForwardStatus(reqmgrSvc, wfStatusDict, logger):

    for status, nextStatus in AUTO_TRANSITION.items():
        count = 0
        requests = reqmgrSvc.getRequestByStatus([status], detail=False)
        for wf in requests:
            stateFromGQ = wfStatusDict.get(wf, None)
            if stateFromGQ in [None, "canceled"]:
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
            for j in range(i + 1):
                count += 1
                reqmgrSvc.updateRequestStatus(wf, nextStatus[j])
                logger.info("%s in %s moved to %s", wf, status, nextStatus[j])
        logger.info("%s requests moved to new state from %s", count, status)
    return


def moveToCompletedForNoWQJobs(reqmgrSvc, globalQSvc, wfStatusDict, logger):
    """
    Handle workflows that have been either aborted or force-completed.
    This will ensure that no global workqueue elements will be left behind.

    :param reqmgrSvc: object instance of the ReqMgr class
    :param globalQSvc: object instance of the WorkQueue class
    :param wfStatusDict: workflow status according to the workqueue elements
    :param logger: a logger object instance
    :return: None object
    """
    for status, nextStatus in CANCEL_AUTO_TRANSITION.items():
        requests = reqmgrSvc.getRequestByStatus([status], detail=False)
        count = 0
        for wflowName in requests:
            stateFromGQ = wfStatusDict.get(wflowName, None)
            if stateFromGQ == "canceled":
                # elements still in CancelRequested, wait for the agent to do his job
                continue
            elif stateFromGQ in ["acquired", "running-open", "running-closed"]:
                # then something went wrong with the workflow abortion/force-completion
                # trigger another cancel request
                logger.info("%s in %s but WQEs in %s, cancelling it again!",
                            wflowName, status, stateFromGQ)
                globalQSvc.cancelWorkflow(wflowName)
            elif stateFromGQ in ["completed", None]:
                # all elements are already in a final state or no longer exist, advance status
                count += 1
                reqmgrSvc.updateRequestStatus(wflowName, nextStatus)
                logger.info("%s in %s moved to %s", wflowName, status, nextStatus)
        logger.info("Total %s: %d", nextStatus, count)


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
        globalQSvc = WorkQueue(config.workqueue_url)

        self.logger.info("Getting GQ data for status check")
        wfStatusDict = globalQSvc.getWorkflowStatusFromWQE()

        self.logger.info("Advancing statuses")
        moveForwardStatus(reqmgrSvc, wfStatusDict, self.logger)
        moveToCompletedForNoWQJobs(reqmgrSvc, globalQSvc, wfStatusDict, self.logger)

        self.logger.info("Done advancing status")

        return
