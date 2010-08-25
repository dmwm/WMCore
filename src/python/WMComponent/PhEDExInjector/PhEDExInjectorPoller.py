#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

Poll the DBSBuffer database and inject files as they are created.
"""

__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.18 2010/04/27 16:25:30 sfoulkes Exp $"
__version__ = "$Revision: 1.18 $"

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.DBS import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription

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
        self.subscribeMSS = getattr(config.PhEDExInjector, "subscribeMSS", False)

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
        self.getUnsubscribed = daofactory(classname = "GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")

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
    
    def createInjectionSpec(self, injectionData):
        """
        _createInjectionSpec_

        Trasform the data structure returned from the database into an XML
        string for the PhEDEx Data Service.  The injectionData parameter must be
        a dictionary keyed by dataset path.  Each dataset path will map to a
        list of blocks, each block being a dict.  The block dicts will have
        three keys: name, is-open and files.  The files key will be a list of
        dicts, each of which have the following keys: lfn, size and checksum.
        The following is an example object:

        {"dataset1":
          {"block1": {"is-open": "y", "files":
            [{"lfn": "lfn1", "size": 10, "checksum": {"cksum": "1234"}},
             {"lfn": "lfn2", "size": 20, "checksum": {"cksum": "4321"}}]}}}
        """
        injectionSpec = XMLDrop.XMLInjectionSpec(self.dbsUrl)

        for datasetPath in injectionData:
            datasetSpec = injectionSpec.getDataset(datasetPath)

            for fileBlockName, fileBlock in injectionData[datasetPath].iteritems():
                blockSpec = datasetSpec.getFileblock(fileBlockName,
                                                     fileBlock["is-open"])

                for file in fileBlock["files"]:
                    blockSpec.addFile(file["lfn"], file["checksum"],
                                      file["size"])

        improv = injectionSpec.save()
        return improv.makeDOMElement().toprettyxml()
    
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
                if self.seMap.has_key("Buffer") and \
                       self.seMap["Buffer"].has_key(siteName):
                    location = self.seMap["Buffer"][siteName]                    
                elif self.seMap.has_key("MSS") and \
                         self.seMap["MSS"].has_key(siteName):
                    location = self.seMap["MSS"][siteName]
                elif self.seMap.has_key("Disk") and \
                         self.seMap["Disk"].has_key(siteName):
                    location = self.seMap["Disk"][siteName]

            if location == None:
                logging.error("Could not map SE %s to PhEDEx node." % \
                              siteName)
                continue

            xmlData = self.createInjectionSpec(uninjectedFiles[siteName])
            injectRes = self.phedex.injectBlocks(location, xmlData, 0, 0)

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

            xmlData = self.createInjectionSpec(migratedBlocks[siteName])
            injectRes = self.phedex.injectBlocks(location, xmlData, 0, 0)

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

    def subscribeDatasets(self):
        """
        _subscribeDatasets_

        Search DBSBuffer for datasets that have not yet been subscribed to mass
        storage and create subscriptions for them.
        """
        if not self.seMap.has_key("MSS"):
            return

        myThread = threading.currentThread()
        unsubscribedDatasets = self.getUnsubscribedDatasets.execute(conn = myThread.transaction.conn,
                                                                    transaction = True)

        for unsubscribedDataset in unsubscribedDatasets:
            datasetPath = unsubscribedDataset["path"]
            seName = unsubscribedDataset["se_name"]

            if not self.seMap["MSS"].has_key(seName):
                logging.error("No MSS node for SE: %s" % seName)
                continue

            newSubscription = PhEDExSubscription(datasetPath, seName, "DataOps",
                                                 custodial = "y", requestOnly = "n")
            
            xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl, 
                                    newSubscription.getDatasetPaths())
            
            self.phedex.subscribe(newSubscription, xmlData)
            
            self.markSubscribed(datasetPath, conn = myThread.transaction.conn,
                                transaction = True)

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

        if self.subscribeMSS:
            self.subscribeDatasets()

        myThread.transaction.commit()
        return
