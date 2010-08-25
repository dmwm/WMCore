#!/usr/bin/env python
"""
The actual taskArchiver algorithm
"""
__all__ = []
__revision__ = "$Id: WorkQueueManagerPoller.py,v 1.4 2009/12/18 21:35:18 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import threading
import logging
import re
import os
import os.path

from subprocess import Popen, PIPE

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WorkQueue import WorkQueue
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

class WorkQueueManagerPoller(BaseWorkerThread):
    """
    Polls for Error Conditions, handles them
    """
    def __init__(self, config):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config
    
    def setup(self, parameters):
        """
        """

        myThread = threading.currentThread()

        self.jsonSender = JSONRequests(self.config.requestMrgHost)
        
        self.workQueue = WorkQueue.localQueue()
        
        if self.config.level == "GlobalQueue":
            self.jsonSender = JSONRequests(self.config.requestMrgHost)
        elif self.config.level == "LocalQueue":
            self.globalQueueDS = WorkQueueDS({'endpoint':self.config.serviceUrl})
        
            
        self.config.serviceUrl = "http://cmssrv18.fnal.gov:6660"
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
        logging.debug("Running algorithm for retrieving workload")
        myThread = threading.currentThread()
        try:
            if self.retrieveCondition():
                if self.config.level == "GlobalQueue":
                    workLoadUrlList = self.retrieveWorkLoadFromReqMgr()
                    parentQueueId = None
                    myThread.transaction.begin()
                    for workLoadUrl in workLoadUrlList:
                        self.workQueue.queueWork(workLoadUrl, parentQueueId)
                        #requestManagner state update call
                    myThread.transaction.commit()
                elif self.config.level == "LocalQueue":
                    wmspecInfoList = self.retrievWorkLoadFromGlobalWorkQ()
                    elementIDs = []
                    myThread.transaction.begin()
                    for wmspecInfo in wmspecInfoList:
                        self.workQueue.queueWork(wmspecInfo['url'], parentQueueId)
                        elementIDs.append(wmspecInfo['element_id'])
                    self.globalQueueDS.status(elementIDs = elementIDs)
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
        requestName = "TestRequest"
        wmAgentUrl = "ralleymonkey.com"
        result = self.jsonSender.post('/reqMgr/assignment/%s/%s' % (requestName, wmAgentUrl))

        return result

    def retrievWorkLoadFromGlobalWorkQ(self):
        
        """
        """
        siteJob = {}
        return self.globalQueueDS.getWork(siteJob)
        
        
        
