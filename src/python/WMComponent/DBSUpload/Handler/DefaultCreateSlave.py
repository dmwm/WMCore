#!/usr/bin/env python
"""
Slave used for default create failure handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultCreateSlave.py,v 1.1 2008/10/08 21:19:34 afaq Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import time
import random

from WMCore.ThreadPool.ThreadSlave import ThreadSlave

class DefaultCreateSlave(ThreadSlave):
    """
    The default slave for a create failure message
    """

    def __call__(self, parameters):
        logging.debug("I am the default create slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        time.sleep(sleepTime) 
