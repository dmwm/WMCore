'''
Created on May 19, 2015

'''
from __future__ import (division, print_function)

from Utils.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.ReqMgr.DataStructs.RequestStatus import AUTO_TRANSITION
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.RequestDB.RequestDBWriter import RequestDBWriter

def moveForwardStatus(reqDBWriter, wfStatusDict):
    
    for status, nextStatus in AUTO_TRANSITION.iteritems():
        requests = reqDBWriter.getRequestByStatus([status])
        for wf in requests:
            stateFromGQ = wfStatusDict.get(wf, None)
            if stateFromGQ is None:
                continue
            elif stateFromGQ == status:
                continue
            elif stateFromGQ == "failed" and status == "assigned":
                reqDBWriter.updateRequestStatus(wf, stateFromGQ)
                print("%s in %s moved to %s" % (wf, status, stateFromGQ))
                continue
            
            try:
                i = nextStatus.index(stateFromGQ)
            except ValueError:
                # No state change needed
                continue
            # special case for aborted workflow - aborted-completed instead of completed
            if status == "aborted" and i == 0:
                reqDBWriter.updateRequestStatus(wf, "aborted-completed")
                print("%s in %s moved to %s" % (wf, status, "aborted-completed"))
            else:
                for j in range(i+1):
                    reqDBWriter.updateRequestStatus(wf, nextStatus[j])
                    print("%s in %s moved to %s" % (wf, status, nextStatus[j]))
    return

def moveToArchivedForNoJobs(reqDBWriter, wfStatusDict):
    '''
    Handle the case when request is aborted/rejected before elements are created in GQ
    '''
    statusTransition = {"aborted": ["aborted-completed", "aborted-archived"],
                        "aborted-completed": ["aborted-archived"],
                        "rejected": ["rejected-archived"]}
    
    for status, nextStatusList in statusTransition.items():
        requests = reqDBWriter.getRequestByStatus([status])
        count = 0
        for wf in requests:
            # check whether wq elements exists for given request
            # if not, it means 
            if wf not in wfStatusDict:
                for nextStatus in nextStatusList: 
                    reqDBWriter.updateRequestStatus(wf, nextStatus)
                    count += 1
        print("Total %s-archived: %d", (status, count))
    
    return
            
            


class StatusChangeTasks(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.advanceStatus, 'duration': config.checkStatusDuration}]

    def advanceStatus(self, config):
        """
        gather active data statistics
        """
        
        reqDBWriter = RequestDBWriter(config.reqmgrdb_url)
        gqService = WorkQueue(config.workqueue_url)
        
        wfStatusDict = gqService.getWorkflowStatusFromWQE()
        
        moveForwardStatus(reqDBWriter, wfStatusDict)
        moveToArchivedForNoJobs(reqDBWriter, wfStatusDict) 
                  
        return
                    