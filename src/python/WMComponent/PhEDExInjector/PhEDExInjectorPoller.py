#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

Poll the DBSBuffer database and inject files as they are created.
"""

import threading
import logging
import traceback
from httplib import HTTPException

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx

from WMCore.DAOFactory import DAOFactory

from WMCore.WMException import WMException

class PhEDExInjectorPassableError(WMException):
    """
    _PassableError_

    Raised in cases where the error is sufficiently severe to terminate
    the loop, but not severe enough to force us to crash the code.

    Built to use with PhEDEx injection failures - if PhEDEx fails we should
    terminate the loop, but continue to retry without terminating the entire
    component.
    """

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
        self.dbsUrl = config.DBSInterface.globalDBSUrl
        self.group = getattr(config.PhEDExInjector, "group", "DataOps")

        # This will be used to map SE names which are stored in the DBSBuffer to
        # PhEDEx node names.  The first key will be the "kind" which consists
        # of one of the following: MSS, Disk, Buffer.  The next key will be the
        # SE name.
        self.seMap = {}
        self.nodeNames = []

        self.diskSites = getattr(config.PhEDExInjector, "diskSites", ["storm-fe-cms.cr.cnaf.infn.it",
                                                                      "srm-cms-disk.gridpp.rl.ac.uk"])

        # initialize the alert framework (if available - config.Alert present)
        #    self.sendAlert will be then be available
        self.initAlerts(compName = "PhEDExInjector")


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
        self.setStatus = daofactory(classname = "DBSBufferFiles.SetPhEDExStatus")

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

        return injectionSpec.save()

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
                if siteName in self.diskSites:
                    if self.seMap.has_key("Disk") and \
                           self.seMap["Disk"].has_key(siteName):
                        location = self.seMap["Disk"][siteName]
                    elif self.seMap.has_key("Buffer") and \
                             self.seMap["Buffer"].has_key(siteName):
                        location = self.seMap["Buffer"][siteName]
                    elif self.seMap.has_key("MSS") and \
                             self.seMap["MSS"].has_key(siteName):
                        location = self.seMap["MSS"][siteName]
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
                msg = "Could not map SE %s to PhEDEx node." % siteName
                logging.error(msg)
                self.sendAlert(7, msg = msg)
                continue

            myThread.transaction.begin()
            xmlData = self.createInjectionSpec(uninjectedFiles[siteName])
            try:
                injectRes = self.phedex.injectBlocks(location, xmlData)
            except Exception, ex:
                # If we get an error here, assume that it's temporary (it usually is)
                # log it, and ignore it in the algorithm() loop
                msg =  "Encountered error while attempting to inject blocks to PhEDEx.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("Traceback: %s" % str(traceback.format_exc()))
                raise PhEDExInjectorPassableError(msg)
            logging.info("Injection result: %s" % injectRes)

            if not injectRes.has_key("error"):
                for datasetName in uninjectedFiles[siteName]:
                    for blockName in uninjectedFiles[siteName][datasetName]:
                        for file in uninjectedFiles[siteName][datasetName][blockName]["files"]:
                            injectedFiles.append(file["lfn"])
            else:
                msg = ("Error injecting data %s: %s" %
                       (uninjectedFiles[siteName], injectRes["error"]))
                logging.error(msg)
                self.sendAlert(6, msg = msg)

            self.setStatus.execute(injectedFiles, 1,
                                   conn = myThread.transaction.conn,
                                   transaction = myThread.transaction)
            injectedFiles = []
            myThread.transaction.commit()

        return

    def closeBlocks(self):
        """
        _closeBlocks_

        Close any blocks that have been migrated to global DBS.
        """
        myThread = threading.currentThread()
        migratedBlocks = self.getMigrated.execute()

        for siteName in migratedBlocks.keys():
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
                msg = "Could not map SE %s to PhEDEx node." % siteName
                logging.error(msg)
                self.sendAlert(6, msg = msg)
                continue

            myThread.transaction.begin()
            try:
                xmlData = self.createInjectionSpec(migratedBlocks[siteName])
                injectRes = self.phedex.injectBlocks(location, xmlData)
                logging.info("Block closing result: %s" % injectRes)
            except HTTPException, ex:
                # If we get an HTTPException of certain types, raise it as an error
                if ex.status == 400:
                    msg =  "Received 400 HTTP Error From PhEDEx: %s" % str(ex.result)
                    logging.error(msg)
                    self.sendAlert(6, msg = msg)
                    logging.debug("Blocks: %s" % migratedBlocks[siteName])
                    logging.debug("XMLData: %s" % xmlData)
                    raise
                else:
                    msg =  "Encountered error while attempting to close blocks in PhEDEx.\n"
                    msg += str(ex)
                    logging.error(msg)
                    logging.debug("Traceback: %s" % str(traceback.format_exc()))
                    raise PhEDExInjectorPassableError(msg)
            except Exception, ex:
                # If we get an error here, assume that it's temporary (it usually is)
                # log it, and ignore it in the algorithm() loop
                msg =  "Encountered error while attempting to close blocks in PhEDEx.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("Traceback: %s" % str(traceback.format_exc()))
                raise PhEDExInjectorPassableError(msg)

            if not injectRes.has_key("error"):
                for datasetName in migratedBlocks[siteName]:
                    for blockName in migratedBlocks[siteName][datasetName]:
                        logging.debug("Closing block %s" % blockName)
                        self.setBlockStatus.execute(blockName, locations = None,
                                                    open_status = "Closed",
                                                    conn = myThread.transaction.conn,
                                                    transaction = myThread.transaction)
            else:
                msg = ("Error injecting data %s: %s" %
                       (migratedBlocks[siteName], injectRes["error"]))
                logging.error(msg)
                self.sendAlert(6, msg = msg)
            myThread.transaction.commit()
        return

    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and attempt to inject them into
        PhEDEx.
        """
        myThread = threading.currentThread()
        try:
            self.injectFiles()
            self.closeBlocks()
        except PhEDExInjectorPassableError, ex:
            logging.error("Encountered PassableError in PhEDExInjector")
            logging.error("Rolling back current transaction and terminating current loop, but not killing component.")
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollbackForError()
            pass
        except Exception:
            # Guess we should roll back if we actually have an exception
            if getattr(myThread, 'transaction', None):
                myThread.transaction.rollbackForError()
            raise

        return
