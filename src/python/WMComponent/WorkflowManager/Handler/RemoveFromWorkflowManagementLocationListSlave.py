#!/usr/bin/env python
#pylint: disable=C0301
"""
Slave used by RemoveFromWorkflowManagementLocationList handler
"""

__all__ = []

import logging
import threading
import pickle

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class RemoveFromWorkflowManagementLocationListSlave(DefaultSlave):
    """
    The default slave for RemoveFromWorkflowManagementLocationList message
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
        if "FilesetMatch" in args and "WorkflowId" in args \
        and "Locations" in args:
            locations = args['Locations'].split(",")
            try:
                myThread.transaction.begin()
                for loc in locations:
                    self.unmarkLocation.execute(workflow = args['WorkflowId'], \
                                                fileset_match = args['FilesetMatch'], \
                                                location = loc, \
                                                conn = myThread.transaction.conn, \
                                                transaction = True)
                myThread.transaction.commit()
            except:
                myThread.transaction.rollback()
                raise
        else:
            logging.error("Received malformed parameters: %s" % str(args))

        # Report as done
        myThread = threading.currentThread()
        myThread.msgService.finish()
