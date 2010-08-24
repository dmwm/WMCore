#!/usr/bin/env python
"""
Slave used for RemoveWorkflowFromManagement handler
"""

__all__ = []
__revision__ = \
    "$Id: RemoveWorkflowFromManagementSlave.py,v 1.3 2009/02/05 18:08:17 jacksonj Exp $"
__version__ = "$Revision: 1.3 $"

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
        logging.debug("Handling RmoveWorkflowFromManagement message: %s" % \
                                                                    str(args))
        
        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId"):
            try:
                transaction.begin()
                self.queries.removeManagedWorkflow(args['WorkflowId'], \
                                                   args['FilesetMatch'])
                transaction.commit()
            except:
                transaction.rollback()
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
