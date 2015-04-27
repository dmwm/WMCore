#!/usr/bin/env
#pylint: disable=W0613
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
        self.fileset_watch = {}
        self.last_poll_time = time.time()/60
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
        fileset_watch_temp = []
        listFileset = {}
        fileset_new = {}

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

            if managedFilesets[fileset]['name'] not in self.fileset_watch:

                self.fileset_watch[filesetToUpdate.name] = filesetToUpdate
                fileset_new[filesetToUpdate.name] = filesetToUpdate

            listFileset[filesetToUpdate.name] = filesetToUpdate

        # Update the list of the fileset to watch
        for oldFileset in self.fileset_watch:

            if oldFileset not in listFileset:

                fileset_watch_temp.append(oldFileset)
        # Remove from the list of the fileset to update the ones which are not
        # in ManagedFilesets anymore
        for oldTempFileset in fileset_watch_temp:
            del self.fileset_watch[oldTempFileset]

        logging.debug("NEW FILESETS %s" %fileset_new)
        logging.debug("OLD FILESETS %s" %self.fileset_watch)

        # WorkQueue work
        for name, fileset in fileset_new.items():

            logging.debug("Will poll %s : %s" % (name, fileset.id))
            self.workq.enqueue(name, fileset)

        for key, filesets in self.workq.__iter__():

            fileset = self.fileset_watch[key]
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
        if ((time.time()/60) - self.last_poll_time) > 10 :

            # WorkQueue handles old filesets
            for name, fileset in self.fileset_watch.items():

                logging.debug("Will poll %s : %s" % (name, fileset.id))
                self.workq.enqueue(name, fileset)

            for key, filesets in self.workq.__iter__():

                fileset = self.fileset_watch[key]
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
            self.last_poll_time = time.time()/60

    # Switch to static method if needed
    def pollExternal(self, fileset):
        """
        Call relevant external source and get file details
        """

        logging.debug("Feeder name %s" % (fileset.name).split(":")[1])
        try:

            factory = WMFactory("default", \
                "WMCore.WMBSFeeder." + (fileset.name).split(":")[1])
            feeder = factory.loadObject( classname = "Feeder", getFromCache = False )
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
