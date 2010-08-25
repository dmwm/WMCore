#!/usr/bin/env python
#pylint: disable-msg=C0301
"""
Slave used for RemoveWorkflowFromManagement handler
"""

__all__ = []


import logging
import threading

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class RemoveWorkflowFromManagementSlave(DefaultSlave):
    """
    The default slave for a RemoveWorkflowFromManagement message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        args = self.messageArgs
        logging.debug("Handling RmoveWorkflowFromManagement message: %s" % \
                                                                    str(args))
        myThread = threading.currentThread()
        
        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId"):
            try:
                myThread.transaction.begin()
                self.queries.removeManagedWorkflow(args['WorkflowId'], \
                                                   args['FilesetMatch'])
                myThread.transaction.commit()
            except:
                myThread.transaction.rollback()
                raise
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread.msgService.finish()
