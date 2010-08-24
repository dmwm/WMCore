#!/usr/bin/env python
"""
Slave used for default run failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultRunSlave.py,v 1.2 2009/01/13 19:35:22 afaq Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import time
import random

from WMCore.ThreadPool.ThreadSlave import ThreadSlave

class DefaultRunSlave(ThreadSlave):
    """
    The default slave for a run failure message
    """

    def __call__(self, parameters):

        logging.debug("I am the default run slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        time.sleep(sleepTime) 


	print "HELLO !!!!!!!!!!!!!!!!!!!!"





