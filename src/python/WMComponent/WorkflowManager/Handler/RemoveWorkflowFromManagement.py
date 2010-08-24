#!/usr/bin/env python
"""
Handler for remove workflow
"""
__all__ = []
__revision__ = "$Id: RemoveWorkflowFromManagement.py,v 1.1 2009/02/05 14:45:03 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class RemoveWorkflowFromManagement(BaseHandler):
    """
    Default handler for removal of workflow / fileset --> workflow mapping
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        
        # Define a slave threadpool
        self.threadpool = ThreadPool(\
            "WMComponent.WorkflowManager.Handler.RemoveWorkflowFromManagementSlave", \
            self.component, 'RemoveWorkflowFromManagement', \
            self.component.config.WorkflowManager.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # Add item to the thread pool and return
        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload})
