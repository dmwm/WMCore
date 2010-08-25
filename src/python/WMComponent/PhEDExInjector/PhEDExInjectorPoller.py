#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

Poll the DBSBuffer database and inject blocks as they are created.
"""

__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.7 2009/09/18 18:53:03 sfoulkes Exp $"
__version__ = "$Revision: 1.7 $"

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx as phedexApi
from WMCore.Services.Requests import JSONRequests
from WMCore.Services.Requests import SSLJSONRequests

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorPoller(BaseWorkerThread):
    """
    _PhEDExInjectorPoller_

    Poll the DBSBuffer database and inject blocks as they are created.
    """
    def __init__(self, config):
        """
        ___init___
        
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.config = config
        self.phedex = phedexApi({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        self.dbsUrl = config.DBSUpload.dbsurl 

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

        self.getUninjected = daofactory(classname = "GetUninjectedBlocks")
        self.setInjected = daofactory(classname = "SetBlocksInjected")

        (protocol, blank, host, path) = self.config.PhEDExInjector.phedexurl.split("/", 3)
        if protocol == "https:":
            cmswebReq = SSLJSONRequests(url = host)
        else:
            cmswebReq = JSONRequest(url = host)

        cmswebResp = cmswebReq.get(uri = "/" + path + "nodes")[0]

        for node in cmswebResp["phedex"]["node"]:
            if not self.seMap.has_key(node["kind"]):
                self.seMap[node["kind"]] = {}

            logging.debug("Adding mapping %s -> %s" % (node["se"], node["name"]))
            self.seMap[node["kind"]][node["se"]] = node["name"]
            self.nodeNames.append(node["name"])

        return

    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for uninjected blocks and attempt to inject them into
        PhEDEx.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        uninjectedBlocks = self.getUninjected.execute()

        injectedBlocks = []
        for uninjectedBlock in uninjectedBlocks:
            # SE names can be stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here. 
            location = None

            if uninjectedBlock["location"] in self.nodeNames:
                location = uninjectedBlock["location"]
            else:
                if self.seMap.has_key("MSS"):
                    if self.seMap["MSS"].has_key(uninjectedBlock["location"]):
                        location = self.seMap["MSS"][uninjectedBlock["location"]]
                elif self.seMap.has_key("Disk"):
                    if self.seMap["Disk"].has_key(uninjectedBlock["location"]):
                        location = self.seMap["Disk"][uninjectedBlock["location"]]

            if location == None:
                logging.error("Could not map SE %s to PhEDEx node." % \
                              uninjectedBlock["location"])
                continue

            logging.debug("Going to inject: %s, %s" % \
                          (location, uninjectedBlock["blockname"]))
            datasetPath = uninjectedBlock["blockname"].rsplit("#")[0]
            injectRes = self.phedex.injectBlocks(self.dbsUrl, location, datasetPath,
                                                 0, 0, uninjectedBlock["blockname"])

            if not injectRes.has_key("error"):
                injectedBlocks.append({"location": uninjectedBlock["location"],
                                       "blockname": uninjectedBlock["blockname"]})
            else:
                logging.error("Error injecting block %s: %s" % \
                              (uninjectBlock["blockname"], injectRes["error"]))

        if len(injectedBlocks) > 0:
            self.setInjected.execute(injectedBlocks,
                                     conn = myThread.transaction.conn,
                                     transaction = myThread.transaction)

        myThread.transaction.commit()
        return
