#!/usr/bin/env python
"""
Slave used for AddWorkflowToManage handler
"""

__all__ = []
__revision__ = \
    "$Id: AddWorkflowToManageSlave.py,v 1.1 2009/02/05 14:45:02 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading
import pickle

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class AddWorkflowToManageSlave(DefaultSlave):
    """
    The default slave for a AddWorkflowToManage message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        logging.debug("Handling AddWorkflowToManage message: %s" % str(params))
        
        # Validate arguments
        args = self.messageArgs
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("SplitAlgo") and args.has_key("Type"):
            self.queries.addManagedWorkflow(args['WorkflowId'], \
                                            args['FilesetMatch'], \
                                            args['SplitAlgo'], \
                                            args['Type'])
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
