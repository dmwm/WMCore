#!/usr/bin/env
#pylint: disable-msg=W0613
"""
Filesets and Feeders manager
"""

__all__ = []
__revision__ = "$Id: FeederManagerPoller.py,\
     v 1.4 2009/11/06 12:08:15 riahi Exp $" 
__version__ = "$Revision: 1.4 $"

import os
import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from ProdCommon.ThreadTools import WorkQueue
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMFactory import WMFactory
from WMCore.WMInit import WMInit

# Tracks filesets watched:{filesetName:filesetObject}
FILESET_WATCH = {}

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
        self.workq = WorkQueue.WorkQueue \
              ([self.pollExternal for _ in range(threads)])

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
        Queries DB for all watched filesets, if information about filesets are
        completed, set the fileset to close (Not implemented yet)
        """
        # Global variable shared between threads 
        global FILESET_WATCH 
        FILESET_WATCH = {}

        # Get All managed filesets 
        managedFilesets = self.queries.getAllManagedFilesets()
        logging.debug("Found %s managed filesets" % len(managedFilesets))

        for fileset in range(len(managedFilesets)):

            # Poll for the new fileset only
            if managedFilesets[fileset]['name'] in FILESET_WATCH:
                logging.debug \
       ('fileset %s is in mn_filesets' % managedFilesets[fileset]['name']) 

            else: 

                logging.debug("Processing %s %s" % \
  ( managedFilesets[fileset]['id'] , managedFilesets[fileset]['name'] ) )
                filesetToUpdate = Fileset(id=managedFilesets[fileset]['id'])

                filesetToUpdate.loadData()
                # lock me!
                FILESET_WATCH[filesetToUpdate.name] = filesetToUpdate

        # WorkQueue worker
        for name, fileset in FILESET_WATCH.items():

            logging.debug("Will poll %s : %s" % (name, fileset.id))
            self.workq.enqueue(name, fileset)

        for key, filesets in self.workq.__iter__():

            fileset = FILESET_WATCH[key]
            logging.debug \
      ("the poll key %s result %s is ready !" % (key,str(fileset.id)))


            feederId = self.queries.getFeederId((fileset.name).split(":")[1])
            logging.debug("the Feeder %s has processed %s and is \
                  removing it if closed" % (feederId , fileset.name ) ) 


            # Finally delete fileset 
            # If the fileset is closed remove it
            if fileset.open == False: 
                self.queries.removeManagedFilesets(fileset.id, feederId)

                # lock me!
                del FILESET_WATCH[key]

    # Switch to static method if needed
    def pollExternal(self, fileset):
        """
        Call relevant external source and get file details
        """

        # Get configuration       
        self.init = WMInit()
        self.init.setLogging()
        self.init.setDatabaseConnection(os.getenv("DATABASE"), \
            os.getenv('DIALECT'), os.getenv("DBSOCK"))


        logging.debug("Feeder name %s" %(fileset.name).split(":")[1])
        try:

            factory = WMFactory("default", \
                "WMCore.WMBSFeeder." + (fileset.name).split(":")[1])
            feeder = factory.loadObject("Feeder")
            feeder(fileset)

        except:

            msg = "Feeder plugin \'%s\' unknown" \
              % (fileset.name).split(":")[1] 
            logging.info(msg)
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



