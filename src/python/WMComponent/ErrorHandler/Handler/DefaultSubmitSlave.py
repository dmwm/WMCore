#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSubmitSlave.py,v 1.2 2008/09/30 18:25:38 fvlingen Exp $"
__version__ = "$Revision: 1.2 $"

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
