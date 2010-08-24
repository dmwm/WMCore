#!/usr/bin/env python
"""
Slave used for AddWorkflowToManagementLocationList handler
"""

__all__ = []
__revision__ = \
    "$Id: AddToWorkflowManagementLocationListSlave.py,v 1.2 2009/02/05 15:47:14 jacksonj Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
import pickle

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class AddToWorkflowManagementLocationListSlave(DefaultSlave):
    """
    The default slave for a AddWorkflowToManagementLocationList message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        args = self.messageArgs
        msg = "Handling AddToWorkflowManagementLocationList message: %s" % \
                                                                    str(args)
        logging.debug(msg)
        
        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("Locations") and args.has_key("Valid"):
            locations = args['Locations'].split(",")
            for loc in locations:
                self.queries.markLocation(args['WorkflowId'], \
                                          args['FilesetMatch'], \
                                          loc, \
                                          args['Valid'])
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
