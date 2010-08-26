#!/usr/bin/env
#pylint: disable-msg=C0301
"""
The workflow manager itself, set up event listeners and work event thread
"""
__all__ = []
__revision__ = "$Id: WorkflowManager.py,v 1.4 2009/02/05 23:26:34 jacksonj Exp $"
__version__ = "$Revision: 1.4 $"

import threading
import logging

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
        pollInterval = self.config.WorkflowManager.pollInterval
        logging.info("Setting poll interval to %s seconds" % pollInterval)
        myThread.workerThreadManager.addWorker(WorkflowManagerPoller(), \
                                               pollInterval)
