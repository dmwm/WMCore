#!/usr/bin/env python
"""
Poll request manager for new work
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerReqMgrPoller.py,v 1.5 2010/05/13 18:43:34 sryu Exp $"
__version__ = "$Revision: 1.5 $"

import threading
import re
import os
import os.path

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

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
        myThread.name = "WorkQueueManagerReqMgrPoller"
        
        self.wq.logger.info("Contacting Request manager for more work %s" % self.reqMgr.__class__)
        if self.retrieveCondition():
            try:
                workLoads = self.retrieveWorkLoadFromReqMgr()
            except Exception, ex:
                msg = "Error contacting RequestManager: %s" % str(ex)
                self.wq.logger.warning(msg)
                return
            if not workLoads:
                self.wq.logger.info("No work retrieved")
                return
            self.wq.logger.debug("work load url list %s" % workLoads.__class__.__name__)
            self.wq.logger.debug(workLoads)
            #TODO: Same functionality as WorkQueue.pullWork() - combine
            work = 0
            for reqName, workLoadUrl in workLoads.items():
                try:
                    self.wq.logger.info("Processing request %s" % reqName)
                    wmspec = WMWorkloadHelper()
                    wmspec.load(workLoadUrl)
                    units = self.wq._splitWork(wmspec)

                    # Process each request in a new transaction - performance hit?
                    # Done as reporting back to ReqMgr is done per request not bulk
                    with self.wq.transactionContext():
                        for unit in units:
                            self.wq._insertWorkQueueElement(unit)
                        try:
                            self.reqMgr.postAssignment(reqName)
                        except Exception, ex:
                            self.wq.logger.error("Unable to update ReqMgr state: %s" % str(ex))
                            self.wq.logger.error('Request "%s" not queued' % reqName)
                            raise

                    work += len(units)
                except Exception, ex:
                    self.wq.logger.exception("Error processing request %s" % reqName)

            self.logger.info("%s element(s) obtained from RequestManager" % work)
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

    # Reuse this when bulk updates supported
#    def sendConfirmationToReqMgr(self, requestNames):
#        """
#        """
#        #TODO: allow bulk post
#        for requestName in requestNames:
#            result = self.reqMgr.postAssignment(requestName)

