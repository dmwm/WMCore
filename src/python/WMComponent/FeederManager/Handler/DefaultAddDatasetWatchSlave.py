#!/usr/bin/env python
"""
Slave used for default AddDatasetWatch behavior
"""

__all__ = []
__revision__ = \
    "$Id: DefaultAddDatasetWatchSlave.py,v 1.2 2009/02/02 23:37:37 jacksonj Exp $"
__version__ = "$Revision: 1.2 $"

import logging
import threading
import pickle

from WMComponent.FeederManager.Handler.DefaultSlave import DefaultSlave

class DefaultAddDatasetWatchSlave(DefaultSlave):
    """
    The default slave for a run failure message
    """
    
    def __init__(self):
        """
        Initialise the slave
        """
        DefaultSlave.__init__(self)

    def __call__(self, parameters):
        """
        Perform the work required with the given parameters
        """
        # Unpickle the parameters
        message = pickle.loads(str(parameters["payload"]))
        
        # Lock on the running feeders list
        myThread = threading.currentThread()
        myThread.runningFeedersLock.acquire()
        
        # Get feeder type
        feederType = message["FeederType"]
        
        # Check if there is a running feeder
        if myThread.runningFeeders.has_key(feederType):
            logging.info("HAVE FEEDER " + feederType + " RUNNING")
            logging.info(myThread.runningFeeders[feederType])
        else:
            logging.info("NO FEEDER " + feederType + " RUNNING")
            
            # Check if we have a feeder in DB
            if self.queries.checkFeeder(feederType):
                # Have feeder, get info
                logging.info("Getting Feeder from DB")
                feederId = self.queries.getFeederId(feederType)
                logging.info(feederId)
                myThread.runningFeeders[feederType] = feederId
            else:
                # Create feeder
                logging.info("Adding Feeder to DB")
                self.queries.addFeeder(feederType, "StatePath")
                feederId = self.queries.getFeederId(feederType)
                logging.info(feederId)
                myThread.runningFeeders[feederType] = feederId
            
        myThread.runningFeedersLock.release()
        
        myThread.msgService.finish()
