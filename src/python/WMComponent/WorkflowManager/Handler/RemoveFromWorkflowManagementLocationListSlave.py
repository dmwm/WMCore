#!/usr/bin/env python
"""
Slave used for RemoveFromWorkflowManagementLocationList handler
"""

__all__ = []
__revision__ = \
    "$Id: RemoveFromWorkflowManagementLocationListSlave.py,v 1.1 2009/02/05 14:45:02 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading
import pickle

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class RemoveFromWorkflowManagementLocationListSlave(DefaultSlave):
    """
    The default slave for a RemoveFromWorkflowManagementLocationList message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        msg = "Handling RemoveFromWorkflowManagementLocationList message: %s" %\
                                                                    str(params)
        logging.debug(msg)
        
        # Validate arguments
        args = self.messageArgs
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("Locations"):
            locations = args['Locations'].split(",")
            for loc in locations:
                self.queries.unmarkLocation(args['WorkflowId'], \
                                          args['FilesetMatch'], \
                                          loc)
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
