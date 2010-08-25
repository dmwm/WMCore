#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

Poll the DBSBuffer database and inject files as they are created.
"""

__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.12 2009/12/02 18:34:46 sfoulkes Exp $"
__version__ = "$Revision: 1.12 $"

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.Requests import SSLJSONRequests

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorPoller(BaseWorkerThread):
    """
    _PhEDExInjectorPoller_

    Poll the DBSBuffer database and inject files as they are created.
    """
    def __init__(self, config):
        """
        ___init___
        
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        self.dbsUrl = config.DBSUpload.globalDBSUrl 

        # This will be used to map SE names which are stored in the DBSBuffer to
        # PhEDEx node names.  The first key will be the "kind" which consists
        # of one of the following: MSS, Disk, Buffer.  The next key will be the
        # SE name.
        self.seMap = {}
        self.nodeNames = []
    
    def setup(self, parameters):
        """
        _setup_
        
        Create a DAO Factory for the PhEDExInjector.  Also load the SE names to
        PhEDEx node name mappings from the data service.
        """
        myThread = threading.currentThread()
        daofactory = DAOFactory(package = "WMComponent.PhEDExInjector.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)

        self.getUninjected = daofactory(classname = "GetUninjectedFiles")
        self.getMigrated = daofactory(classname = "GetMigratedBlocks")

        daofactory = DAOFactory(package = "WMComponent.DBSBuffer.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)   
        self.setStatus = daofactory(classname = "DBSBufferFiles.SetStatus")

        daofactory = DAOFactory(package = "WMComponent.DBSUpload.Database",
                                logger = self.logger,
                                dbinterface = myThread.dbi)   
        self.setBlockStatus = daofactory(classname = "SetBlockStatus")        

        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:
            if not self.seMap.has_key(node["kind"]):
                self.seMap[node["kind"]] = {}

            logging.info("Adding mapping %s -> %s" % (node["se"], node["name"]))
            self.seMap[node["kind"]][node["se"]] = node["name"]
            self.nodeNames.append(node["name"])

        return

    def injectFiles(self):
        """
        _injectFiles_

        Inject any uninjected files in PhEDEx.
        """
        myThread = threading.currentThread()
        uninjectedFiles = self.getUninjected.execute()

        injectedFiles = []
        for siteName in uninjectedFiles.keys():
            # SE names can be stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here. 
            location = None

            if siteName in self.nodeNames:
                location = siteName
            else:
                if self.seMap.has_key("MSS"):
                    if self.seMap["MSS"].has_key(siteName):
                        location = self.seMap["MSS"][siteName]
                elif self.seMap.has_key("Disk"):
                    if self.seMap["Disk"].has_key(siteName):
                        location = self.seMap["Disk"][siteName]

            if location == None:
                logging.error("Could not map SE %s to PhEDEx node." % \
                              siteName)
                continue

            injectRes = self.phedex.injectBlocksFromDB(self.dbsUrl,
                                                       uninjectedFiles[siteName],
                                                       location, 1, 0)

            if not injectRes.has_key("error"):
                for datasetName in uninjectedFiles[siteName]:
                    for blockName in uninjectedFiles[siteName][datasetName]:
                        for file in uninjectedFiles[siteName][datasetName][blockName]["files"]:
                            injectedFiles.append(file["lfn"])
            else:
                logging.error("Error injecting data %s: %s" % \
                              (uninjectedFiles[siteName], injectRes["error"]))

        if len(injectedFiles) > 0:
            logging.debug("Injecting files: %s" % injectedFiles)
            self.setStatus.execute(injectedFiles, "InPhEDEx", 
                                     conn = myThread.transaction.conn,
                                     transaction = myThread.transaction)

        return

    def closeBlocks(self):
        """
        _closeBlocks_

        Close any blocks that have been migrated to global DBS.
        """
        myThread = threading.currentThread()
        migratedBlocks = self.getMigrated.execute()

        closedBlocks = []
        for siteName in migratedBlocks.keys():
            # SE names can be stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here. 
            location = None

            if siteName in self.nodeNames:
                location = siteName
            else:
                if self.seMap.has_key("MSS"):
                    if self.seMap["MSS"].has_key(siteName):
                        location = self.seMap["MSS"][siteName]
                elif self.seMap.has_key("Disk"):
                    if self.seMap["Disk"].has_key(siteName):
                        location = self.seMap["Disk"][siteName]

            if location == None:
                logging.error("Could not map SE %s to PhEDEx node." % \
                              siteName)
                continue

            injectRes = self.phedex.injectBlocksFromDB(self.dbsUrl,
                                                       migratedBlocks[siteName],
                                                       location, 1, 0)

            if not injectRes.has_key("error"):
                for datasetName in migratedBlocks[siteName]:
                    for blockName in migratedBlocks[siteName][datasetName]:
                        closedBlocks.append(blockName)
            else:
                logging.error("Error injecting data %s: %s" % \
                              (uninjectedFiles[siteName], injectRes["error"]))

        for closedBlock in closedBlocks:
            logging.debug("Closing block %s" % closedBlock)
            self.setBlockStatus.execute(closedBlock, locations = None,
                                        open_status = "Closed", 
                                        conn = myThread.transaction.conn,
                                        transaction = myThread.transaction)


        return
        
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and attempt to inject them into
        PhEDEx.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        self.injectFiles()
        self.closeBlocks()

        myThread.transaction.commit()
        return
