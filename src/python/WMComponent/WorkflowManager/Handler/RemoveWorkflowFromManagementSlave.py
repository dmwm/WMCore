#!/usr/bin/env python
"""
Slave used for RemoveWorkflowFromManagement handler
"""

__all__ = []
__revision__ = \
    "$Id: RemoveWorkflowFromManagementSlave.py,v 1.2 2009/02/05 15:47:14 jacksonj Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
import pickle

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class RemoveWorkflowFromManagementSlave(DefaultSlave):
    """
    The default slave for a RemoveWorkflowFromManagement message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        args = self.messageArgs
        logging.debug("Handling RmoveWorkflowFromManagement message: %s" % str(args))
        
        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId"):
            self.queries.removeManagedWorkflow(args['WorkflowId'], \
                                               args['FilesetMatch'])
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
