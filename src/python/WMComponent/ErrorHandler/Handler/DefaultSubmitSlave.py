#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultSubmitSlave.py,v 1.1 2008/09/12 13:02:09 fvlingen Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import time
import random

from WMCore.ThreadPool.ThreadSlave import ThreadSlave

class DefaultSubmitSlave(ThreadSlave):
    """
    The default slave for a submit failure message
    """

    def __call__(self, parameters):
        logging.debug("I am the default submit slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        time.sleep(sleepTime) 
