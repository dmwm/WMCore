#!/usr/bin/env python

import threading
import logging
import re
from sets import Set

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.WMFactory import WMFactory

from WMCore.WMBS.Subscription import Subscription
from WMCore.WMBS.Fileset import Fileset
from WMCore.WMBS.Workflow import Workflow

class WorkflowManagerPoller(BaseWorkerThread):
    """
    Regular worker for the WorkflowManager. Constructs subscriptions as filesets
    that have been added for watching become available
    """
    def setup(self, parameters):
        """
        Load DB objects required for queries
        """
        myThread = threading.currentThread()
        factory = WMFactory("default", \
            "WMComponent.WorkflowManager.Database." + myThread.dialect)
        self.queries = factory.loadObject("Queries")
    
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        logging.info("Running subscription / fileset matching algorithm")
        
        # Get all watched mappings
        managedWorkflows = self.queries.getManagedWorkflows()
        if len(workflows == 0):
            return
        
        # Get the details of all unsubscribed filesets
        availableFilesets = self.queries.getUnsubscribedFilesets()
            
        # Match filesets to managed workflows
        for fileset in availableFilesets:
            # Fileset object cache to pass into Subscription constructor
            fsObj = None
            
            for managedWorkflow in managedWorkflows:
                # Workflow object cache
                wfObj = None
                
                # Load the location information
                whitelist = Set()
                blacklist = Set()
                locations = self.queries.getLocations(managedWorkflow['id'])
                for location in locations:
                    logging.info(str(location))
                    if location['valid'] == True:
                        whitelist.add(location['se_name'])
                    else:
                        whitelist.add(location['se_name'])
                
                # Attempt to match workflows to filesets
                if re.match(managedWorkflow['fileset_match'], fileset['name']):
                    # Match found - Load the fileset if not already loaded
                    if not fsObj:
                        fsObj = Fileset(id = fileset['id'])
                        fsObj.load()
                      
                    # Load the workflow if not already loaded
                    if not wfObj:
                        wfObj = Workflow(id = managedWorkflow['workflow'])
                        wfObj.load()
                        
                    # Create the subscription
                    logging.info("Creating subscription for fileset %s" % \
                                                            fileset['name'])
                    newSub = Subscription(fileset = fsObj, \
                                     workflow = wfObj, \
                                     whitelist = whitelist, \
                                     blacklist = blacklist, \
                                     split_algo = managedWorkflow['split_algo'],
                                     type = managedWorkflow['type'])
                    newSub.create()
