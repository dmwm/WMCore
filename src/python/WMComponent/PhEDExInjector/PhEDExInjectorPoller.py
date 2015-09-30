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
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription

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

        self.blocksToRecover = None

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
        
        daofactory = DAOFactory(package = "WMComponent.DBS3Buffer",
                                logger = self.logger,
                                dbinterface = myThread.dbi)
        self.setStatus = daofactory(classname = "DBSBufferFiles.SetPhEDExStatus")
        self.setBlockClosed = daofactory(classname = "SetBlockClosed")

        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:
            if node["kind"] not in self.seMap:
                self.seMap[node["kind"]] = {}

            logging.info("Adding mapping %s -> %s" % (node["se"], node["name"]))
            self.seMap[node["kind"]][node["se"]] = node["name"]
            self.nodeNames.append(node["name"])

        return

    def createInjectionSpec(self, injectionData):
        """
        _createInjectionSpec_

        Transform the data structure returned from the database into an XML
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

    def createRecoveryFileFormat(self, unInjectedData):
        """
        _createRecoveryFileFormat_

        Transform the data structure returned from database in to the dict format
        for the PhEDEx Data Service.  The injectionData parameter must be
        a dictionary keyed by dataset path.  
        
        unInjectedData format
        {"dataset1":
          {"block1": {"is-open": "y", "files":
            [{"lfn": "lfn1", "size": 10, "checksum": {"cksum": "1234"}},
             {"lfn": "lfn2", "size": 20, "checksum": {"cksum": "4321"}}]}}}
        
        returns
        [{"block1": set(["lfn1", "lfn2"])}, {"block2": set(["lfn3", "lfn4"])]
        """
        blocks = []
        for datasetPath in unInjectedData:

            for blockName, fileBlock in unInjectedData[datasetPath].items():

                newBlock = { blockName : set() }

                for fileDict in fileBlock["files"]:
                    newBlock[blockName].add(fileDict["lfn"])

                blocks.append(newBlock)

        return blocks
    
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
                    if "Disk" in self.seMap and \
                           siteName in self.seMap["Disk"]:
                        location = self.seMap["Disk"][siteName]
                    elif "Buffer" in self.seMap and \
                             siteName in self.seMap["Buffer"]:
                        location = self.seMap["Buffer"][siteName]
                    elif "MSS" in self.seMap and \
                             siteName in self.seMap["MSS"]:
                        location = self.seMap["MSS"][siteName]
                else:
                    if "Buffer" in self.seMap and \
                           siteName in self.seMap["Buffer"]:
                        location = self.seMap["Buffer"][siteName]
                    elif "MSS" in self.seMap and \
                             siteName in self.seMap["MSS"]:
                        location = self.seMap["MSS"][siteName]
                    elif "Disk" in self.seMap and \
                             siteName in self.seMap["Disk"]:
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
            except HTTPException as ex:
                # If we get an HTTPException of certain types, raise it as an error
                if ex.status == 400:
                    # assume it is duplicate injection error. but if that is not the case
                    # needs to be investigated
                    self.blocksToRecover = self.createRecoveryFileFormat(uninjectedFiles[siteName])
                
                msg = "PhEDEx injection failed with %s error: %s" % (ex.status, ex.result)
                raise PhEDExInjectorPassableError(msg)
            except Exception as ex:
                # If we get an error here, assume that it's temporary (it usually is)
                # log it, and ignore it in the algorithm() loop
                msg =  "Encountered error while attempting to inject blocks to PhEDEx.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("Traceback: %s" % str(traceback.format_exc()))
                
                raise PhEDExInjectorPassableError(msg)
            logging.info("Injection result: %s" % injectRes)

            if "error" not in injectRes:
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
    
    def subscribeBlocksInDataset(self, datasetPaths, node, xmlData):
        """
        _subscribeBlocksInDataset_
        :arg list datasetPaths: list of datasetPath
        :arg str node: site location
        :arg str xmlData: xml formatted data
        """
        
        subs = []

        # Create the subscription objects and add them to the list
        # The list takes care of the sorting internally 
        for datasetPath in datasetPaths:
            phedexSub = PhEDExSubscription(datasetPath, node, self.group, level = 'block',
                                           move='n', custodial='n', static='y',
                                           request_only='n', no_mail='y')
            # Add it to the list
            subs.append(phedexSub)

        for subscription in subs:
            
            msg = "Subscribing: %s to %s, with options: " % (subscription.getDatasetPaths(), subscription.getNodes())
            msg += "Move: %s, Custodial: %s, Request Only: %s, Level: %s,  No mail: %s" % (
                                subscription.move, subscription.custodial, subscription.request_only, 
                                subscription.level, subscription.no_mail)
            logging.debug(msg)
            self.phedex.subscribe(subscription, xmlData)
            
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
                if "Buffer" in self.seMap and \
                       siteName in self.seMap["Buffer"]:
                    location = self.seMap["Buffer"][siteName]
                elif "MSS" in self.seMap and \
                         siteName in self.seMap["MSS"]:
                    location = self.seMap["MSS"][siteName]
                elif "Disk" in self.seMap and \
                         siteName in self.seMap["Disk"]:
                    location = self.seMap["Disk"][siteName]

            if location == None:
                msg = "Could not map SE %s to PhEDEx node." % siteName
                logging.error(msg)
                self.sendAlert(6, msg = msg)
                continue

            myThread.transaction.begin()
            try:
                xmlData = self.createInjectionSpec(migratedBlocks[siteName])
                # inject and subscribe when closing the block
                subRes = self.subscribeBlocksInDataset(migratedBlocks[siteName].keys(), location, xmlData)
                injectRes = self.phedex.injectBlocks(location, xmlData)
                logging.info("Block closing result: %s" % injectRes)
            except HTTPException as ex:
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
            except Exception as ex:
                # If we get an error here, assume that it's temporary (it usually is)
                # log it, and ignore it in the algorithm() loop
                msg =  "Encountered error while attempting to close blocks in PhEDEx.\n"
                msg += str(ex)
                logging.error(msg)
                logging.debug("Traceback: %s" % str(traceback.format_exc()))
                raise PhEDExInjectorPassableError(msg)

            if "error" not in injectRes:
                for datasetName in migratedBlocks[siteName]:
                    for blockName in migratedBlocks[siteName][datasetName]:
                        logging.debug("Closing block %s" % blockName)
                        self.setBlockClosed.execute(blockName,
                                                    conn = myThread.transaction.conn,
                                                    transaction = myThread.transaction)
            else:
                msg = ("Error injecting data %s: %s" %
                       (migratedBlocks[siteName], injectRes["error"]))
                logging.error(msg)
                self.sendAlert(6, msg = msg)
            myThread.transaction.commit()
        return

    def recoverInjectedFiles(self):
        """
        When PhEDEx inject call timed out, run this function.
        Since there are 3 min reponse time out in cmsweb, some times 
        PhEDEx injection call times out even though the call succeeded
        In that case run the recovery mode
        1. first check whether files which injection status = 0 are in the PhEDEx.
        2. if those file exist set the in_phedex status to 1
        3. set self.blocksToRecover = None

        Run this recovery one block at a time, with too many blocks
        the call to the PhEDEx data service on cmsweb can time out
        """
        myThread = threading.currentThread()

        # recover one block at a time
        for block in self.blocksToRecover:

            injectedFiles = self.phedex.getInjectedFiles(block)

            if len(injectedFiles) > 0:

                myThread.transaction.begin()
                self.setStatus.execute(injectedFiles, 1)
                myThread.transaction.commit()
                logging.info("%s files already injected: changed status in dbsbuffer db" % len(injectedFiles))

        self.blocksToRecover = None
        return
        
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected files and attempt to inject them into
        PhEDEx.
        """
        myThread = threading.currentThread()
        try:
            if self.blocksToRecover != None:
                logging.info(""" Running PhEDExInjector Recovery: 
                                 previous injection call failed, 
                                 check if files were injected to PhEDEx anyway""")
                self.recoverInjectedFiles()
                        
            self.injectFiles()
            self.closeBlocks()
        except PhEDExInjectorPassableError as ex:
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
