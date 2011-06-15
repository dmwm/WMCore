#!/usr/bin/env python
"""
_PhEDExInjectorSubscriber_

Poll the DBSBuffer database and subscribe datasets to MSS as they are created.
"""

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorSubscriber(BaseWorkerThread):
    """
    _PhEDExInjectorSubscriber_

    Poll the DBSBuffer database and subscribe datasets to MSS as they are
    created.
    """
    def __init__(self, config):
        """
        ___init___
        
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        self.dbsUrl = config.DBSInterface.globalDBSUrl 
        self.group = getattr(config.PhEDExInjector, "group", "DataOps")

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

        self.getUnsubscribed = daofactory(classname = "GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")

        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:
            if not self.seMap.has_key(node["kind"]):
                self.seMap[node["kind"]] = {}

            logging.info("Adding mapping %s -> %s" % (node["se"], node["name"]))
            self.seMap[node["kind"]][node["se"]] = node["name"]
            self.nodeNames.append(node["name"])

        return
    
    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for datasets and subscribe them to MSS.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        if not self.seMap.has_key("MSS"):
            return

        unsubscribedDatasets = self.getUnsubscribed.execute(conn = myThread.transaction.conn,
                                                            transaction = True)

        datasetMap = {}
        for unsubscribedDataset in unsubscribedDatasets:
            datasetPath = unsubscribedDataset["path"]
            seName = unsubscribedDataset["se_name"]

            if not self.seMap["MSS"].has_key(seName):
                logging.error("No MSS node for SE: %s" % seName)
                continue

            if not datasetMap.has_key(self.seMap["MSS"][seName]):
                datasetMap[self.seMap["MSS"][seName]] = []
            datasetMap[self.seMap["MSS"][seName]].append(datasetPath)

            self.markSubscribed.execute(datasetPath, conn = myThread.transaction.conn,
                                        transaction = True)

        for siteMSS in datasetMap.keys():
            logging.info("Subscribing %s to %s" % (datasetMap[siteMSS],
                                                   siteMSS))
            newSubscription = PhEDExSubscription(datasetMap[siteMSS], siteMSS, self.group,
                                                 custodial = "y", requestOnly = "n")
            
            xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl, 
                                                       newSubscription.getDatasetPaths())
            print xmlData
            self.phedex.subscribe(newSubscription, xmlData)

        myThread.transaction.commit()
        return
