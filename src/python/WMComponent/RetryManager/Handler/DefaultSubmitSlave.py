#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSubmitSlave.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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

        # Discover the jobs that are in submit cooloff step (with status 'submitcooloff')
        jobs = listSubmitCooloff.execute()
        # Retries < max retry count
        for ajob in jobs:
                # $TYPECooloff -> $STATE
                # Cooled off condition met, reset to $STATE
                # (bunch of cool off algorithms to be developed - plugins)
        myThread.msgService.finish()
        time.sleep(sleepTime)

