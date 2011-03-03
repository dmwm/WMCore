#!/usr/bin/env
#pylint: disable-msg=W0613
"""
Filesets and Feeders manager
"""

__all__ = []

import threading
import logging
import traceback
import time

from WMCore.DAOFactory import DAOFactory
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.ThreadPool.WorkQueue import ThreadPool
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMFactory import WMFactory

# Tracks filesets in dict. using this format filesetName:filesetObject}
FILESET_WATCH = {}
FILESET_NEW = {}
LONG_SLEEP = time.time()/60

class FeederManagerPoller(BaseWorkerThread):
    """
    Regular managed fileset poller, instantiate feeder
    by fileset
    """

    def __init__(self, threads=8):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.workq = ThreadPool \
              ([self.pollExternal for _ in range(threads)])

    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        daofactory = DAOFactory(package = "WMComponent.FeederManager.Database" , \
              logger = myThread.logger, \
              dbinterface = myThread.dbi)

        self.getAllManagedFilesets = daofactory(classname = "GetAllManagedFilesets")
        self.getFeederId = daofactory(classname = "GetFeederId")
        self.removeManagedFilesets = daofactory(classname = "RemoveManagedFilesets")


    def databaseWork(self):
        """
        completed, set the fileset to close (Not implemented yet)
        """
        # Global variable shared between threads
        # We don't need global variables anymore - see ticket #875
        global FILESET_WATCH
        global FILESET_NEW
        global LONG_SLEEP

        FILESET_WATCH_temp = []
        listFileset = {}
        FILESET_NEW = {}

        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Get All managed filesets
        managedFilesets = self.getAllManagedFilesets.execute(\
        conn = myThread.transaction.conn, transaction = True)

        myThread.transaction.commit()

        logging.debug("Found %s managed filesets" % len(managedFilesets))

        for fileset in range(len(managedFilesets)):

            logging.debug("Processing %s %s" % \
  ( managedFilesets[fileset]['id'] , managedFilesets[fileset]['name'] ) )

            filesetToUpdate = Fileset(id=managedFilesets[fileset]['id'])
            filesetToUpdate.load()

            if managedFilesets[fileset]['name'] not in FILESET_WATCH:

                FILESET_WATCH[filesetToUpdate.name] = filesetToUpdate
                FILESET_NEW[filesetToUpdate.name] = filesetToUpdate

            listFileset[filesetToUpdate.name] = filesetToUpdate

        # List update
        for oldFileset in FILESET_WATCH:

            if oldFileset not in listFileset:

                FILESET_WATCH_temp.append(oldFileset)

        for oldTempFileset in FILESET_WATCH_temp:
            del FILESET_WATCH[oldTempFileset]

        logging.debug("NEW FILESETS %s" % FILESET_NEW)
        logging.debug("OLD FILESETS %s" % FILESET_WATCH)


        # WorkQueue work
        for name, fileset in FILESET_NEW.items():

            logging.debug("Will poll %s : %s" % (name, fileset.id))
            self.workq.enqueue(name, fileset)

        for key, filesets in self.workq.__iter__():

            fileset = FILESET_WATCH[key]

            logging.debug \
      ("the poll key %s result %s is ready !" % (key, str(fileset.id)))

            myThread.transaction.begin()

            feederId = self.getFeederId.execute( \
    feederType = (fileset.name).split(":")[1], \
conn = myThread.transaction.conn, transaction = True )

            myThread.transaction.commit()

            logging.debug("the Feeder %s has processed %s and is \
                  removing it if closed" % (feederId, fileset.name) )


            # Finally delete fileset
            # If the fileset is closed remove it
            fileset.load()
            if fileset.open == False:

                myThread.transaction.begin()
                self.removeManagedFilesets.execute( \
filesetId = fileset.id, feederType = feederId, \
        conn = myThread.transaction.conn, transaction = True )
                myThread.transaction.commit()

        # Handles old filesets. We update old filesets every 10 mn
        # We need to make old filesets update cycle configurable
        if ((time.time()/60) - LONG_SLEEP) > 10 :

            # WorkQueue handles old filesets
            for name, fileset in FILESET_WATCH.items():

                logging.debug("Will poll %s : %s" % (name, fileset.id))
                self.workq.enqueue(name, fileset)

            for key, filesets in self.workq.__iter__():

                fileset = FILESET_WATCH[key]

                logging.debug \
          ("the poll key %s result %s is ready !" % (key, str(fileset.id)))

                myThread.transaction.begin()
                feederId = self.getFeederId.execute(\
           feederType = (fileset.name).split(":")[1], \
     conn = myThread.transaction.conn, transaction = True )
                myThread.transaction.commit()

                logging.debug("the Feeder %s has processed %s and is \
                      removing it if closed" % (feederId, fileset.name) )


                # Finally delete fileset
                # If the fileset is closed remove it
                fileset.load()
                if fileset.open == False:

                    myThread.transaction.begin()
                    self.removeManagedFilesets.execute(\
        filesetId = fileset.id, feederType = feederId, \
    conn = myThread.transaction.conn, transaction = True )
                    myThread.transaction.commit()

            # Update the last update time of old filesets
            LONG_SLEEP = time.time()/60

    # Switch to static method if needed
    def pollExternal(self, fileset):
        """
        Call relevant external source and get file details
        """

        logging.debug("Feeder name %s" % (fileset.name).split(":")[1])
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
