#!/usr/bin/env python
"""
Poll request manager for new work
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerReqMgrPoller.py,v 1.1 2010/02/12 14:34:37 swakef Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import re
import os
import os.path


from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WorkQueue import WorkQueue
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.RequestManager.RequestManager \
     import RequestManager as RequestManagerDS

class WorkQueueManagerReqMgrPoller(BaseWorkerThread):
    """
    Polls for requests
    """
    def __init__(self, reqMgr, queue, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.reqMgr = reqMgr
        self.wq = queue
        self.config = config

    def algorithm(self, parameters):
        """
        retrive workload (workspec) from RequestManager
	"""
        myThread = threading.currentThread()
        self.wq.logger.info("Contacting Request manager for more work")
        if self.retrieveCondition():
            try:
                workLoads = self.retrieveWorkLoadFromReqMgr()
            except StandardError, ex:
                msg = "Error contacting RequestManager: %s" % str(ex)
                self.wq.logger.warning(msg)
                return
            if not workLoads:
                self.wq.logger.info("No work retrieved")
                return
            self.wq.logger.info("work load url list %s" % workLoads.__class__.__name__)
            self.wq.logger.info(workLoads)
            #TODO: Same functionality as WorkQueue.pullWork() - combine
            work, units = [], []
            for workLoadUrl in workLoads.values():
                wmspec = WMWorkloadHelper()
                wmspec.load(workLoadUrl)
                work.extend(self.wq_splitWork(wmspec))

            self.wq.logger.info("Converted to work: %s" % str(work))
            myThread.transaction.begin()
            for unit in work:
                # shouldn't be calling this method, add similar public api
                units.append(self.wq._insertWorkQueueElement(unit))
            myThread.transaction.commit()
                
            try:
                self.sendConfirmationToReqMgr(workLoads.keys())
            except StandardError, ex:
                self.wq.logger.error("Unable to update ReqMgr state: %s" % str(ex))
                #TODO: Warning if this fails it is not retried - must fix
                #Temporarilly fail obtained units
                #FIXME: Another issue sendConfirmation is not atomic!!
                self.wq.logger.error("Cancelling obtained work")
                self.wq.cancelWork(units)

        return

    def retrieveCondition(self):
        """
        _retrieveCondition_
        set true or false for given retrieve condion
        i.e. thredshod on workqueue 
        """
        return True
    
    def retrieveWorkLoadFromReqMgr(self):
        """
        retrieveWorkLoad
        retrieve list of url for workloads.
        """
        #requestName = "TestRequest"
        #requestName = 'rpw_100122_145356'
        #wmAgentUrl = "ralleymonkey.com"
        result = self.reqMgr.getAssignment(self.config.get('teamName', ''))
        return result
        
        
    def sendConfirmationToReqMgr(self, requestNames):
        """
        """
        #TODO: allow bulk post
        for requestName in requestNames:
            result = self.reqMgr.postAssignment(requestName)
        
