#!/usr/bin/env python
"""
_PhEDExInjectorPoller_

"""

__revision__ = "$Id: PhEDExInjectorPoller.py,v 1.5 2009/09/08 18:28:15 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx as phedexApi
from WMCore.Services.Requests import JSONRequests

from WMCore.DAOFactory import DAOFactory

class PhEDExInjectorPoller(BaseWorkerThread):
    """
    _PhEDExInjectorPoller_

    """
    def __init__(self, config, noclue = None):
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

        cmswebReq = JSONRequests(url = "cmsweb.cern.ch")
        cmswebResp = cmswebReq.get(uri = "/phedex/datasvc/json/prod/nodes")[0]

        for node in cmswebResp:
            if not seMap.has_key(node["kind"]):
                seMap[node["kind"]] = {}

            seMap[node["kind"]][node["se"]] = node["name"]

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
            # SE names are stored in DBSBuffer as that is what is returned in
            # the framework job report.  We'll try to map the SE name to a
            # PhEDEx node name here. 
            location = None
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
                    
            result = self.phedex.injectBlocks(self.dbsUrl,
                                              uninjectedBlock["location"],
                                              uninjectedBlock["blockname"])

            if not result.has_key("error"):
                injectedBlocks.append({"location": uninjectedBlock["location"],
                                       "blockname": uninjectedBlock["blockname"])
            else:
                logging.error("Error injecting block %s: %s" % \
                              (uninjectBlock["blockname"], result["error"]))

        setAction.execute(injectedBlocks, conn = myThread.transaction.conn,
                          transaction = myThread.transaction)

        myThread.transaction.commit()
        return
