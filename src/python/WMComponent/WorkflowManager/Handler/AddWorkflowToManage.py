#!/usr/bin/env python
#pylint: disable-msg=C0301,R0903
"""
Handler for add workflow
"""
__all__ = []



from WMCore.Agent.BaseHandler import BaseHandler
from WMCore.ThreadPool.ThreadPool import ThreadPool

class AddWorkflowToManage(BaseHandler):
    """
    Default handler for addition of workflow / fileset --> workflow mapping
    """

    def __init__(self, component):
        BaseHandler.__init__(self, component)
        
        # Define a slave threadpool
        self.threadpool = ThreadPool(\
            "WMComponent.WorkflowManager.Handler.AddWorkflowToManageSlave", \
            self.component, 'AddWorkflowToManage', \
            self.component.config.WorkflowManager.maxThreads)

     # this we overload from the base handler
    def __call__(self, event, payload):
        """
        Handles the event with payload, by sending it to the threadpool.
        """
        # Add item to the thread pool and return

        self.threadpool.enqueue(event, {'event' : event, 'payload' :payload['payload']})
