#!/usr/bin/env python
"""
Slave used for default create cooloff handler.
"""

__all__ = []
__revision__ = \
    "$Id: DefaultCreateSlave.py,v 1.1 2009/05/11 16:49:04 afaq Exp $"
__version__ = "$Revision: 1.1 $"

import logging
import threading
import time
import random

# inherit from our default slave implementation
from WMComponent.ErrorHandler.Handler.DefaultSlave import DefaultSlave

class DefaultCreateSlave(DefaultSlave):
    """
    The default slave for a create failure message
    """

    def __call__(self, parameters):
        # first call the super class to do the default work.
        DefaultSlave.__call__(self, parameters)

        logging.debug("I am the default create slave called with parameters:"\
            +str(parameters))
        sleepTime = random.randint(0, 5)
        myThread = threading.currentThread()

        # we need to do this in our slave otherwise the failure
        # messages that might have been published, will not be send.

	# Discover the jobs that are in create cooloff step (with status 'createcooloff')
	jobs = listCreateCooloff.execute()
	# Retries < max retry count
	for ajob in jobs:
		# $TYPECooloff -> $STATE	 
		# Cooled off condition met, reset to $STATE 
		# (bunch of cool off algorithms to be developed - plugins)	
        myThread.msgService.finish()
        time.sleep(sleepTime) 

