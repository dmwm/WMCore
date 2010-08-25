#!/usr/bin/env python
"""
The actual taskArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerPoller.py,v 1.7 2010/01/26 21:52:48 sryu Exp $"
__version__ = "$Revision: 1.7 $"

import threading
import logging
import re
import os
import os.path


from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WorkQueue import WorkQueue
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.RequestManager.RequestManager \
     import RequestManager as RequestManagerDS

class WorkQueueManagerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        
        self.wqManagerConfig = config.WorkQueueManager
        
        self.agentConfig = config.Agent
        
        
    def setup(self, parameters):
        """
        _setup_
        setting up member variables for workqueue poller
        WorkQueue Manager poller can be used for polling information from RequestManager
        or GlobalQueue (remote workqueue) depending on the parameter configuration
        """

        myThread = threading.currentThread()

        if self.wqManagerConfig.level == "GlobalQueue":
            logging.info("Global Queue Manager Started" )
            self.jsonSender = JSONRequests(self.wqManagerConfig.serviceUrl)
            self.rqMgrDS = RequestManagerDS()
            self.workQueue = WorkQueue.globalQueue(**self.wqManagerConfig.queueParams)
        elif self.wqManagerConfig.level == "LocalQueue":
            logging.info("Local Queue Manager Started" )
            self.workQueue = WorkQueue.localQueue(**self.wqManagerConfig.queueParams)
        else:
            raise Exception, "WorkQueue level needs to be set eigther GlobalQueue or LocalQueue"
        return


    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        return

    def algorithm(self, parameters):
        """
        retrive workload (workspec) from the src. either from upper queue or
        RequestManager
	    """
        logging.info("Running algorithm for retrieving workload")
        myThread = threading.currentThread()
        try:
            if self.retrieveCondition():
                if self.wqManagerConfig.level == "GlobalQueue":
                    
                    workLoads = self.retrieveWorkLoadFromReqMgr()
                    logging.info("work load url list %s" % workLoads.__class__.__name__)
                    logging.info(workLoads)
                    parentQueueId = None
                    myThread.transaction.begin()
                    for workLoadUrl in workLoads.values():
                        logging.info("workLoadUrl %s" % workLoadUrl)
                        self.workQueue.queueWork(workLoadUrl, parentQueueId)
                        #requestManagner state update call
                    myThread.transaction.commit()
                    # TODO: needs to handle failing status correctly.
                    # Maybe maintain the status for request manager confirmation staus 
                    self.sendConfirmationToReqMgr(workLoads.keys())
                    #Not sure this is needed (separate transaction)
                    self.workQueue.updateLocationInfo()
                    
                elif self.wqManagerConfig.level == "LocalQueue":
                    # (separate transaction)
                    self.workQueue.updateLocationInfo()
                    
                    myThread.transaction.begin()
                    # pullwork needs to be tested
                    self.workQueue.pullWork()
                    myThread.transaction.commit()
                    
        except:
            raise

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
        requestName = 'rpw_100122_145356'
        wmAgentUrl = "ralleymonkey.com"
        #result = self.jsonSender.post('/reqMgr/assignment/%s/%s' % (requestName, wmAgentUrl))
        #TODO:hard coded for the test remove this
        self.agentConfig.teamName = 'Dodgers'
        #result = self.jsonSender.get('/reqMgr/assignment/%s' % self.agentConfig.teamName)
        result = self.rqMgrDS.getAssignment(self.agentConfig.teamName)
        return result
        
        
    def sendConfirmationToReqMgr(self, requestNames):
        """
        """
        #TODO: allow bulk post
        for requestName in requestNames:
            #result = self.jsonSender.get('/reqMgr/assignment/%s' % requestName)
            result = self.rqMgrDS.postAssignment(requestName)
        