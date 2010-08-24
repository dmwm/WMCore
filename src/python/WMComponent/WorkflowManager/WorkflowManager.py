#!/usr/bin/env
import logging
import threading

from WMCore.Agent.Harness import Harness
from WMCore.WMFactory import WMFactory

from WMComponent.WorkflowManager.WorkflowManagerPoller import WorkflowManagerPoller

class WorkflowManager(Harness):
    """
    _WorkflowManager_
    
    Watches for filesets matching a given Regex, and creates subscriptions
    using an existing workflow when filesets become available
    """

    def __init__(self, config):
        # call the base class
        Harness.__init__(self, config)
    
    def preInitialization(self):
        """
        Add required worker modules to work threads
        """
        # in case nothing was configured we have a fallback.
        if not hasattr(self.config.FeederManager, "addDatasetWatch"):
            logging.warning("Using default AddDatasetWatch handler")
            self.config.FeederManager.addDatasetWatchHandler =  \
                'WMComponent.FeederManager.Handler.DefaultAddDatasetWatch'

        # use a factory to dynamically load handlers.
        factory = WMFactory('generic')
        self.messages['AddWorkflowToManage'] = \
            factory.loadObject(\
                "WMComponent.WorkflowManager.Handler.AddWorkflowToManage", self)
        self.messages['RemoveWorkflowFromManagement'] = \
            factory.loadObject(\
                "WMComponent.WorkflowManager.Handler.RemoveWorkflowFromManagement", self)
        self.messages['AddToWorkflowManagementLocationList'] = \
            factory.loadObject(\
                "WMComponent.WorkflowManager.Handler.AddToWorkflowManagementLocationList", self)
        self.messages['RemoveFromWorkflowManagementLocationList'] = \
            factory.loadObject(\
                "WMComponent.WorkflowManager.Handler.RemoveFromWorkflowManagementLocationList", self)
        
        # Add event loop to worker manager
        myThread = threading.currentThread()
        myThread.workerThreadManager.addWorker(WorkflowManagerPoller())
