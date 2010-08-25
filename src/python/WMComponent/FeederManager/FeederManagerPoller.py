#!/usr/bin/env
#pylint: disable-msg=W0613
"""
Filesets and Feeders manager
"""

__all__ = []
__revision__ = "$Id: FeederManagerPoller.py,\
     v 1.4 2009/11/06 12:08:15 riahi Exp $" 
__version__ = "$Revision: 1.6 $"

import threading
import logging
import traceback
import time

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
#from ProdCommon.ThreadTools import WorkQueue
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMFactory import WMFactory

# Tracks filesets watched:{filesetName:filesetObject}
FILESET_WATCH = {}
FILESET_NEW = {}
LONG_SLEEP = time.time()/60 
SHORT_SLEEP = time.time()/60 

class FeederManagerPoller(BaseWorkerThread):
    """
    Regular managed fileset poller, instantiate feeder
    by fileset
    """
   
    def __init__(self, threads=5):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queries = None

    def setup(self, parameters):
        """
        Load DB objects required for queries
        """ 
        myThread = threading.currentThread()
        factory = WMFactory("default", \
            "WMComponent.FeederManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")

    def databaseWork(self):
        """
        completed, set the fileset to close (Not implemented yet)
        """

        # Global variable shared between threads 
        global FILESET_WATCH 
        global FILESET_NEW
        global LONG_SLEEP 

        FILESET_NEW = {}

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Get All managed filesets 
        managedFilesets = self.queries.getAllManagedFilesets()
        myThread.transaction.commit()

        logging.debug("Found %s managed filesets" % len(managedFilesets))

        for fileset in range(len(managedFilesets)):

            logging.debug("Processing %s %s" % \
  ( managedFilesets[fileset]['id'] , managedFilesets[fileset]['name'] ) )

            if managedFilesets[fileset]['name'] not in FILESET_WATCH:

                filesetToUpdate = Fileset(id=managedFilesets[fileset]['id'])
                filesetToUpdate.loadData()

                FILESET_WATCH[filesetToUpdate.name] = filesetToUpdate
                FILESET_NEW[filesetToUpdate.name] = filesetToUpdate
        
        logging.debug("NEW FILESETS %s" %FILESET_NEW)
        logging.debug("OLD FILESETS%s" %FILESET_WATCH)

        # new fileset queries 
        for name, fileset in FILESET_NEW.items():

            logging.debug("Polling %s : %s" % (name, fileset.id))
            self.pollExternal(fileset)


            feederId = self.queries.getFeederId((fileset.name).split(":")[1])
            myThread.transaction.commit()

            logging.debug("the Feeder %s has processed %s and is \
                  removing it if closed" % (feederId , fileset.name ) ) 


            # Finally delete fileset 
            # If the fileset is closed remove it
            if fileset.open == False: 

                myThread.transaction.begin()
                self.queries.removeManagedFilesets(fileset.id, feederId)
                myThread.transaction.commit()

            
            logging.debug("Sleeping for 60 SEC after processing %s\
                      " %fileset.name)
            time.sleep(60)

        if ((time.time()/60) - LONG_SLEEP) > 60 :

            # Long pause queries 
            for name, fileset in FILESET_WATCH.items():

                logging.debug("Will poll %s : %s" % (name, fileset.id))
                self.pollExternal(fileset)


                myThread.transaction.begin()
                feederId = self.queries.getFeederId((fileset.name\
                          ).split(":")[1])
                myThread.transaction.commit()

                logging.debug("the Feeder %s has processed %s and is \
                      removing it if closed" % (feederId , fileset.name ) )


                # Finally delete fileset
                # If the fileset is closed remove it
                if fileset.open == False:

                    myThread.transaction.begin()
                    self.queries.removeManagedFilesets(fileset.id, feederId)
                    myThread.transaction.commit()

                time.sleep(60)

            # Update short_sleep var
            LONG_SLEEP = time.time()/60

            #myThread.transaction.commit()


    # Switch to static method if needed
    def pollExternal(self, fileset):
        """
        Call relevant external source and get file details
        """

        logging.debug("Feeder name %s" %(fileset.name).split(":")[1])
        try:
 
            factory = WMFactory("default", \
                "WMCore.WMBSFeeder." + (fileset.name).split(":")[1])
            feeder = factory.loadObject("Feeder")
            feeder(fileset)

        except Exception,e :

            msg = "Feeder plugin \'%s\' unknown" \
              % (fileset.name).split(":")[1] 
            logging.info(msg)
            logging.info(e)
            logging.info( traceback.format_exc() )
            logging.info("aborting poll for...closing fileset")
            fileset.markOpen(False)
            fileset.commit()

        return fileset 

    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if information about filesets are
        completed delete it from watched filesets (set the fileset to close)
        """
        logging.debug("Running feeder / fileset completion algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.databaseWork()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise



