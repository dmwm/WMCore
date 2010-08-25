#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSubmitSlave.py,v 1.3 2009/05/11 14:59:45 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import logging
import threading
import time
import random

from WMComponent.ErrorHandler.Handler.DefaultSlave import DefaultSlave

class DefaultSubmitSlave(DefaultSlave):
    """
    The default slave for a submit failure message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)
        logging.debug("I am the default submit slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        myThread = threading.currentThread()
        # we need to do this in our slave otherwise the failure
        # messages that might have been published, will not be send.
        myThread.msgService.finish()

        time.sleep(sleepTime)

        # Discover the jobs that failed in submit step (with status 'submitfailed')
        jobs = listSubmitFailed.execute()
        # Retries < max retry count
        for ajob in jobs:
                # Retries < max retry count
                if ajob['retry_count'] < ajob['retry_amx']:
                        #SIMON's CODE SHOULD PUT the job in "exhausted" state
                # Check if Retries >= max retry count
                if ajob['retry_count'] >= ajob['retry_max']:
                        #SIMON's CODE SHOULD PUT the job in "retrycooloff" state
        myThread.msgService.finish()

        time.sleep(sleepTime)
 
