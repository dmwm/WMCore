#!/usr/bin/env python
"""
Slave used for default AddDatasetWatch behavior
"""

__all__ = []
__revision__ = \
    "$Id: DefaultAddDatasetWatchSlave.py,v 1.1 2009/02/02 23:06:49 jacksonj Exp $"
__version__ = "$Revision: 1.1 $"

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
        
        logging.info(str(myThread.runningFeeders))
        logging.info(message["FeederType"])
        
        # Check if there is a running feeder
        if myThread.runningFeeders.has_key(message["FeederType"]):
            logging.info("HAVE FEEDER " + message["FeederType"])
        else:
            logging.info("NO FEEDER " + message["FeederType"])
            ret = self.queries.checkFeeder(message["FeederType"])
            logging.info(str(type(ret)))
            logging.info(str(ret))
            
            # Create feeder
            #self.queries.addFeeder(message["FeederType"], "StatePath")
            
            #ret = self.queries.checkFeeder(message["FeederType"])
            #logging.info(str(type(ret)))
            #logging.info(str(ret))
        
        myThread.msgService.finish()
