#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultJobSlave.py,v 1.2 2009/05/08 16:35:52 afaq Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
import time
import random

from WMComponent.ErrorHandler.Handler.DefaultSlave import DefaultSlave

class DefaultJobSlave(DefaultSlave):
    """
    The default slave for a run failure message
    """

    def __call__(self, parameters):
        DefaultSlave.__call__(self, parameters)
        logging.debug("I am the default job slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        myThread = threading.currentThread()
        # we need to do this in our slave otherwise the failure 
        # messages that might have been published, will not be send.
        myThread.msgService.finish()
        time.sleep(sleepTime) 
