#!/usr/bin/env python
"""
Poll request manager for new work
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerReqMgrPoller.py,v 1.11 2010/06/25 16:14:57 sryu Exp $"
__version__ = "$Revision: 1.11 $"

import re
import os
import os.path
import time

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
        # on restart send updates for last day
        self.lastReport = int(time.time()) - 24*60*60

    def algorithm(self, parameters):
        """
        retrive workload (workspec) from RequestManager
	    """
        
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
                            self.wq._insertWorkQueueElement(unit, requestName = reqName)
                        try:
                            self.reqMgr.putWorkQueue(reqName, 
                                            self.wq.params.get('monitorURL', ''))
                        except Exception, ex:
                            # added for debuging but should be removed since remote call 
                            # doesn't make send to trace the stack.
                            #import traceback
                            #self.wq.logger.error("Something Wrong %s" % traceback.format_exc())
                            self.wq.logger.error("Unable to update ReqMgr state: %s" % str(ex))
                            self.wq.logger.error('Request "%s" not queued' % reqName)
                            raise

                    work += len(units)
                except Exception, ex:
                    self.wq.logger.exception("Error processing request %s" % reqName)

            self.logger.info("%s element(s) obtained from RequestManager" % work)

            self.reportToReqMgr()
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

    def reportToReqMgr(self):
        """Report request status to ReqMgr"""
        now = int(time.time())
        try:
            elements = self.wq.status(after = self.lastReport)
            for ele in elements:
                args = {'percent_complete' : ele['PercentComplete'],
                        'percent_success' : ele['PercentSuccess']}
                self.reqMgr.reportRequestProgress(ele['RequestName'], **args)

                status = None
                if ele.isComplete():
                    status = 'completed'
                elif ele.isFailed():
                    status = 'failed'
                elif ele.isRunning():
                    status = 'running'
                self.reqMgr.reportRequestStatus(ele['RequestName'], status)
        except RuntimeError, ex:
            self.wq.logger.warning('Error reporting to ReqMgr: %s' % str(ex))
        else:
            self.lastReport = now