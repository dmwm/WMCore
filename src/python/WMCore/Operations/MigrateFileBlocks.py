#!/usr/bin/env python
"""
Code for migrating blocks to global DBS
"""




import threading
import logging
import re
import os
import time
import sys

import inspect

from WMCore.WMFactory  import WMFactory
from WMCore.DAOFactory import DAOFactory
from WMCore.WMInit     import WMInit

from DBSAPI.dbsApi import DbsApi

from WMQuality.TestInit import TestInit

from WMComponent.DBSBuffer.Database.Interface.DBSBufferFile import DBSBufferFile

from WMCore.Agent.Configuration import loadConfigurationFile
from WMCore.Agent.Configuration import Configuration

from WMCore.Services.DBS.DBSWriter import DBSWriter
from WMCore.Services.DBS           import DBSWriterObjects
from WMCore.Services.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from WMCore.Services.DBS.DBSReader import DBSReader



def usage():
    """
    Exactly what it says on the tin
    """

    print """Usage:
    python MigrateFileBlocks.py  [dataset path] [OPTIONAL: path to config file for DBSUpload]

    where [dataset path] is a formal dataset path (/Primary/Processed/Tier)
    and the config file is probably the DBSUpload default config
    If path is not specified, uses WMAGENT_CONFIG out of the environment"""


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
            self.dbinterface.setBlockStatus(name, locations = None, openStatus = 0)


        return


if __name__ == '__main__':

    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    datasetPath = sys.argv[1]

    if len(sys.argv) < 3:  #No config path sent along
        configPath = os.getenv('WMAGENT_CONFIG', None)
    else:
        configPath = sys.argv[2]
        if not os.path.isfile(configPath):
            configPath = os.getenv('WMAGENT_CONFIG', None)

    if configPath:
        config = loadConfigurationFile(configPath)
    else:
        print "Error!  No config could be found"
        sys.exit(2)
        
        
    wmInit = WMInit()
    wmInit.setLogging()
    wmInit.setDatabaseConnection(config.CoreDatabase.connectUrl,
                                 config.CoreDatabase.dialect,
                                 config.CoreDatabase.socket)

    migrateFileBlocks = MigrateFileBlocks(config = config)
    migrateFileBlocks.migrateDataset(datasetPath = datasetPath)

