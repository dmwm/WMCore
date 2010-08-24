#!/usr/bin/env python
#pylint: disable-msg=C0301
"""
Slave used for RemoveWorkflowFromManagement handler
"""

__all__ = []
__revision__ = \
    "$Id: RemoveWorkflowFromManagementSlave.py,v 1.4 2009/02/05 23:21:43 jacksonj Exp $"
__version__ = "$Revision: 1.4 $"

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
