#!/usr/bin/env python
"""
Slave used for default AddDatasetWatch behavior
"""

__all__ = []
__revision__ = \
"$Id: DefaultAddDatasetWatchSlave.py,v 1.8 2010/05/31 22:41:56 riahi Exp $"
__version__ = "$Revision: 1.8 $"
__author__ = \
    "james.jackson@cern.ch"

import logging
import threading

from WMComponent.FeederManager.Handler.DefaultSlave import DefaultSlave
from WMCore.WMBS.Fileset import Fileset

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
        DefaultSlave.__call__(self, parameters)

        # Handle the message
        message = self.messageArgs

        # Lock on the running feeders list
        myThread = threading.currentThread()
        myThread.runningFeedersLock.acquire()
        
        # Create empty fileset if fileset.name doesn't exist
        filesetName = message["dataset"] 
        feederType = message["FeederType"]
        fileType = message["FileType"]
        startRun = message["StartRun"]

        logging.debug("Dataset " + filesetName + " arrived")
 
        fileset = Fileset(name = filesetName+':'\
          +feederType+':'+fileType+':'+startRun)

        # Check if the fileset is already there 
        if fileset.exists() == False:

            # Empty fileset creation
            fileset.create()
            fileset.setLastUpdate(0)

            logging.info("Fileset %s whith id %s is added" \
                               %(fileset.name, str(fileset.id)))
 
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

            # Fileset/Feeder association 
            self.queries.addFilesetToManage(fileset.id, \
                          myThread.runningFeeders[feederType])
            logging.info("Fileset %s is added to feeder %s" %(fileset.id, \
                          myThread.runningFeeders[feederType]))
        else:

            # If fileset already exist a new subscription 
            # will be created for its workflow       
            logging.info("Fileset exists: Subscription will be created for it")

            # Open it if close
            fileset.load()
            if fileset.open == False:

                fileset.markOpen(True)

                logging.info("Getting Feeder from DB")
                feederId = self.queries.getFeederId(feederType)
                logging.info(feederId)
                myThread.runningFeeders[feederType] = feederId

                self.queries.addFilesetToManage(fileset.id, \
                                  myThread.runningFeeders[feederType])
                logging.info("Fileset %s is added to feeder %s" %(fileset.id, \
                                  myThread.runningFeeders[feederType]))
 
        myThread.runningFeedersLock.release()
        myThread.msgService.finish()
