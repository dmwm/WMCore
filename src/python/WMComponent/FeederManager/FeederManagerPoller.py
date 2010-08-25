#!/usr/bin/env
#pylint: disable-msg=W0613
"""
Filesets and Feeders manager
"""
__all__ = []
__revision__ = "$Id: FeederManagerPoller.py,v 1.1 2009/07/14 13:37:22 riahi Exp $" 
__version__ = "$Revision: 1.1 $"

import threading
import logging
from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.WMFactory import WMFactory
from WMCore.WMBSFeeder.DBS.Feeder import Feeder
from WMCore.WMBS.Fileset import Fileset
#from Registry import retrieveFeederImpl, RegistryError
from ProdCommon.ThreadTools import WorkQueue

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
            # FIXME: Add (and the fileset is closed)  
            if managedFilesets[fileset]['name'] in FILESET_WATCH:
                logging.debug \
       ('fileset %s is in mn_filesets' % managedFilesets[fileset]['name']) 

            else: 

                logging.debug("I m processing %s %s" % \
  ( managedFilesets[fileset]['id'] , managedFilesets[fileset]['name'] ) )
                filesetToUpdate = Fileset(id=managedFilesets[fileset]['id'])
                filesetToUpdate.loadData()
                logging.debug("%s is now processed:" % filesetToUpdate.name)
                # lock me!
                FILESET_WATCH[filesetToUpdate.name] = filesetToUpdate



        # WorkQueue worker
        for name, fileset in FILESET_WATCH.items():

            logging.debug("Will poll %s : %s" % (name, fileset.id))
            self.workq.enqueue(name, fileset)
            logging.debug("%s is enqueued" % name)

        for key, filesets in self.workq.__iter__():

            fileset = FILESET_WATCH[key]
            logging.debug \
      ("the poll key %s result %s is ready !" % (key,fileset.id))

            # Finally delete fileset 
            # lock me!
            del FILESET_WATCH[key]

    # Switch to static method if needed
    def pollExternal(self, fileset):
        """
        Call relevant external source and get file details
        """
        # Looking for!  
        ##try:
             # fileset.source can be dataset feeder
        ##     feeder = retrieveFeederImpl(fileset.source)
        ##except RegistryError:
        ##     msg = "WMBSFeeder plugin \'%s\' unknown" % fileset.source
        ##     logging.error(msg)
        ##     logging.error("aborting poll for...")
        ##     raise RuntimeError, msg
    
        # do we have any parents we need
        ##if fileset.parents and fileset.listNewFiles()\ 
                 # and not fileset.listNewFiles()[0].parents(): 
        # get parentage from dbs
        ##     try:
        ##         parentFeeder = retrieveFeederImpl('dbs', fileset)
        ##     except RegistryError:
        ##         msg = "WMBSFeeder plugin \'%s\' unknown" % 'dbs'
        ##         logging.error(msg)
        ##         logging.error("aborting poll for...")
        ##         raise RuntimeError, msg
        
        ##fileset = parentFeeder.getParentsForNewFiles(fileset)

        # FIXME: Get it more generique
        feederId = self.queries.getFeederId("Feeder")
        logging.debug("the Feeder %s is processing %s" % \
             (feederId , fileset.name ) )
        logging.debug("Feeder called for %s" % fileset.name)
        feeder = Feeder()
        try:
            feeder(fileset)
            self.queries.removeManagedFilesets(fileset.id, feederId)
        except:
            logging.debug("ERROR when calling Feeder")

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



