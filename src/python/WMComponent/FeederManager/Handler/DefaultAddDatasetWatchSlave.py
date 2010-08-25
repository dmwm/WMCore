#!/usr/bin/env python
"""
Slave used for default AddDatasetWatch behavior
"""

__all__ = []
__revision__ = \
"$Id: DefaultAddDatasetWatchSlave.py,v 1.7 2010/05/04 22:28:32 riahi Exp $"
__version__ = "$Revision: 1.7 $"
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


            if feederType == 'DBS':
                filesetBase = Fileset(name = filesetName+':'\
                         +feederType)
                if filesetBase.exists() == False:
                    filesetBase.create()

                    # Fileset/Feeder association
                    self.queries.addFilesetToManage(filesetBase.id, \
                                  myThread.runningFeeders[feederType])
                    logging.info("Fileset %s is added to feeder %s" \
                %(filesetBase.id, myThread.runningFeeders[feederType])) 
                else:
                    logging.info("Fileset Base %s is already there" \
                                  %filesetBase.name)


            # Fileset/Feeder association 
            self.queries.addFilesetToManage(fileset.id, \
                          myThread.runningFeeders[feederType])
            logging.info("Fileset %s is added to feeder %s" %(fileset.id, \
                          myThread.runningFeeders[feederType]))
        else:
            # If fileset already exist a new subscription 
            # will be created for its workflow       
            logging.info("Fileset exists: Subscription will be created for it")
 
        myThread.runningFeedersLock.release()
        
        myThread.msgService.finish()
