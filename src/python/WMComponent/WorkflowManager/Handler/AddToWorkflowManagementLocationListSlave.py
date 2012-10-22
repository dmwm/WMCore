#!/usr/bin/env python
#pylint: disable-msg=C0301
"""
Slave used for AddWorkflowToManagementLocationList handler
"""
__all__ = []

import logging
import threading

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
        myThread = threading.currentThread()

        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("Locations") and args.has_key("Valid"):
            locations = args['Locations'].split(",")
            try:
                myThread.transaction.begin()
                for loc in locations:
                    self.markLocation.execute(workflow = args['WorkflowId'], \
                                              fileset_match = args['FilesetMatch'], \
                                              location = loc, \
                                              valid = args['Valid'], \
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
