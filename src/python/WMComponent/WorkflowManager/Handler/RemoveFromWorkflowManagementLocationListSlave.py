#!/usr/bin/env python
#pylint: disable-msg=C0301
"""
Slave used for RemoveFromWorkflowManagementLocationList handler
"""

__all__ = []



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
        args = self.messageArgs
        msg = "Handling RemoveFromWorkflowManagementLocationList message: %s" % \
                                                                    str(args)
        logging.debug(msg)
        myThread = threading.currentThread()
        
        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("Locations"):
            locations = args['Locations'].split(",")
            try:
                myThread.transaction.begin()
                for loc in locations:
                    self.queries.unmarkLocation(args['WorkflowId'], \
                                                args['FilesetMatch'], \
                                                loc)
                myThread.transaction.commit()
            except:
                myThread.transaction.rollback()
                raise
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
