#!/usr/bin/env python
"""
Slave used by AddWorkflowToManage handler
"""
__all__ = []

import logging
import threading

from WMComponent.WorkflowManager.Handler.DefaultSlave import DefaultSlave

class AddWorkflowToManageSlave(DefaultSlave):
    """
    The default slave for a AddWorkflowToManage message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        args = self.messageArgs
        logging.debug("Handling AddWorkflowToManage message: %s" % str(args))
        myThread = threading.currentThread()

        # Validate arguments
        if args.has_key("FilesetMatch") and args.has_key("WorkflowId") \
        and args.has_key("SplitAlgo") and args.has_key("Type"):
            try:
                myThread.transaction.begin()
                self.addManagedWorkflow.execute(workflow = args['WorkflowId'], \
                                                fileset_match =  args['FilesetMatch'], \
                                                split_algo =  args['SplitAlgo'], \
                                                type =  args['Type'], \
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
