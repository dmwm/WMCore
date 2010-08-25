#!/usr/bin/env python
"""
Code for migrating blocks to global DBS
"""

__revision__ = "$Id: MigrateFileBlocks.py,v 1.1 2009/12/02 19:46:34 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import threading
import logging
import re
import os
import time
import sys

import inspect

from WMCore.WMFactory  import WMFactory
from WMCore.DAOFactory import DAOFactory

from DBSAPI.dbsApi import DbsApi

from WMQuality.TestInit import TestInit

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.DBS           import DBSWriterObjects
from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader



class MigrateFileBlocks:
    """
    Migrate blocks to global DBS

    """


    def __init__(self, config):

        myThread = threading.currentThread()
        
        self.config     = config
        self.dbsurl     = self.config.DBSUpload.dbsurl
        self.dbsversion = self.config.DBSUpload.dbsversion
        self.uploadFileMax = self.config.DBSUpload.uploadFileMax

        self.DBSMaxFiles     = self.config.DBSUpload.DBSMaxFiles
        self.DBSMaxSize      = self.config.DBSUpload.DBSMaxSize
        self.DBSBlockTimeout = self.config.DBSUpload.DBSBlockTimeout
        self.dbswriter = DBSWriter(self.dbsurl, level='ERROR', user='NORMAL', version=self.dbsversion, \
                                   globalDBSUrl  = self.config.DBSUpload.globalDBSUrl, \
                                   globalVersion =  self.config.DBSUpload.globalDBSVer)

        self.daoFactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                     logger = logging,
                                     dbinterface = myThread.dbi)


        factory = WMFactory("dbsUpload", "WMComponent.DBSUpload.Database.Interface")
        self.dbinterface=factory.loadObject("UploadToDBS")



    def migrateDataset(self, datasetPath = None):
        """
        Migrate a Dataset to globalDBS

        """

        if not datasetPath:
            return None

        action = self.daoFactory(classname = "GetBlockFromDataset")
        blockNames = action.execute(dataset = datasetPath)

        for entry in blockNames:
            name = entry['blockname']
            self.dbswriter.manageFileBlock(name, maxFiles = 0, maxSize = None, timeOut = None)
            dbinterface.setBlockStatus(name, locations, block['OpenForWriting'])


        return


if __name__ == '__main__':
    configPath  = sys.argv[2]
    datasetPath = sys.argv[1]

    testInit = TestInit(__file__)
    testInit.setLogging()
    testInit.setDatabaseConnection()

    config = testInit.getConfiguration(configPath)

    migrateFileBlocks = MigrateFileBlocks(config = config)
    migrateFileBlocks.migrateDataset(datasetPath = datasetPath)

