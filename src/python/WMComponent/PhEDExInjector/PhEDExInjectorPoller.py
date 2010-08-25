#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The PhEDExInjector algorithm
"""
__all__ = []
__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.3 2009/08/13 23:58:47 meloam Exp $"
__version__ = "$Revision: 1.3 $"
__author__ = "mnorman@fnal.gov"

import threading
import logging
import re
import os
import time
from sets import Set

import inspect

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory import WMFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

from DBSAPI.dbsApi import DbsApi
#from DBSAPI.dbsException import *
#from DBSAPI.dbsApiException import *
#from DBSAPI.dbsAlgorithm import DbsAlgorithm

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.DBS           import DBSWriterObjects
from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader



class PhEDExInjectorPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config):
        """
        Initialise class members
        """
        myThread = threading.currentThread()
        myThread.dialect = os.getenv('DIALECT')
        BaseWorkerThread.__init__(self)
        self.config     = config
        #self.dbsurl     = self.config.DBSUpload.dbsurl
        #self.dbsversion = self.config.DBSUpload.dbsversion
        self.uploadFileMax = 10
    
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()

        # The WMBS base class creates a DAO factory for WMBS, we'll need to
        # overwrite that so we can use the factory for PhEDExInjector objects.
        self.daofactory = DAOFactory(package = "WMComponent.PhEDExInjector.Database",
                                     logger = self.logger,
                                     dbinterface = self.dbi)
#        bufferFactory = WMFactory("dbsBuffer", "WMComponent.DBSBuffer.Database.Interface")
#        self.addToBuffer=bufferFactory.loadObject("AddToBuffer")

#        logging.info("DBSURL %s"%self.dbsurl)
#        args = { "url" : self.dbsurl, "level" : 'ERROR', "user" :'NORMAL', "version" : self.dbsversion }
#        self.dbsapi = DbsApi(args)
#        self.dbswriter = DBSWriter(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion)

        return

    def inject(self):
        print "Oh yeah, we're injecting"
        action = self.daofactory(classname = "GetUninjectedBlocks")
        print action.execute( conn = self.getDBConn(),
                              transaction = self.existingTransaction())
        
        pass
    
    def terminate(self,params):
        logging.debug("terminating. doing one more pass before we die")
        self.algorithm(params)
        
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions. Wraps in transaction.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.inject()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise

