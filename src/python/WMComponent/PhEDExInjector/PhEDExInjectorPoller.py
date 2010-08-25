#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The PhEDExInjector algorithm
"""
__all__ = []
__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.4 2009/08/24 11:10:03 meloam Exp $"
__version__ = "$Revision: 1.4 $"
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
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx as phedexApi
from WMCore.DAOFactory import DAOFactory


class PhEDExInjectorPoller(BaseWorkerThread):
    """
    Handles poll-based DBSUpload

    """


    def __init__(self, config, noclue = None):
        """
        Initialise class members
        """
        myThread = threading.currentThread()
        myThread.dialect = os.getenv('DIALECT')
        BaseWorkerThread.__init__(self)
        self.config     = config
        self.phedex     = phedexApi({'endpoint': config.PhEDExInjector.phedexurl}, 'json' )
        self.dbsUrl     = config.DBSUpload.dbsurl 
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
                                     dbinterface = myThread.dbi)

        return

    def inject(self):
        myThread = threading.currentThread()

        action = self.daofactory(classname = "GetUninjectedBlocks")
        setAction = self.daofactory(classname = "SetBlocksInjected")
        rows = action.execute( )
        print rows
        condensed = {}
        for row in rows:
            if not row['location'] in condensed:
                condensed[row['location']] = []
            condensed[row['location']].append(row['blockname'])
            self.phedex.injectBlocks(self.dbsUrl, row['location'], row['blockname'])
            setAction.execute( [ {'location': row['location'], 'blockname':row['blockname']} ],
                               conn= myThread.transaction.conn,
                               transaction=myThread.transaction )
            
#        for loc,blocks in condensed.iteritems():
#            print self.phedex.injectBlocks(self.dbsUrl, 
#                                         loc,
#                                         0,
#                                         1,
#                                         *blocks)   
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

