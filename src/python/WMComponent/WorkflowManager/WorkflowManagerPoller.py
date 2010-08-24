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
    
    def databaseWork(self):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions
        """
        # Get all watched mappings
        managedWorkflows = self.queries.getManagedWorkflows()
        logging.debug("Found %s managed workflows" % len(managedWorkflows))
        
        # Get the details of all unsubscribed filesets
        availableFilesets = self.queries.getUnsubscribedFilesets()
        logging.debug("Found %s unsubscribed filesets" % len(availableFilesets))
            
        # Match filesets to managed workflows  
        for managedWorkflow in managedWorkflows:
            # Workflow object cache to pass into Subscription constructor
            wfObj = None
            
            for fileset in availableFilesets:
                # Fileset object cache
                fsObj = None

                # Load the location information
                whitelist = Set()
                blacklist = Set()
                locations = self.queries.getLocations(managedWorkflow['id'])
                for location in locations:
                    if bool(int(location['valid'])) == True:
                        whitelist.add(location['se_name'])
                    else:
                        blacklist.add(location['se_name'])
                logging.info(str(whitelist))
                logging.info(str(blacklist))
                
                # Attempt to match workflows to filesets
                if re.match(managedWorkflow['fileset_match'], fileset['name']):
                    # Log in debug
                    msg = "Creating subscription for %s to workflow id %s"
                    msg %= (fileset['name'], managedWorkflow['workflow'])
                    logging.debug(msg)
                    
                    # Match found - Load the fileset if not already loaded
                    if not fsObj:
                        fsObj = Fileset(id = fileset['id'])
                        fsObj.load()
                      
                    # Load the workflow if not already loaded
                    if not wfObj:
                        wfObj = Workflow(id = managedWorkflow['workflow'])
                        wfObj.load()
                        
                    # Create the subscription
                    newSub = Subscription(fileset = fsObj, \
                                     workflow = wfObj, \
                                     whitelist = whitelist, \
                                     blacklist = blacklist, \
                                     split_algo = managedWorkflow['split_algo'],
                                     type = managedWorkflow['type'])
                    newSub.create()
    
    def algorithm(self, parameters):
        """
        Queries DB for all watched filesets, if matching filesets become
        available, create the subscriptions. Wraps in transaction.
        """
        logging.debug("Running subscription / fileset matching algorithm")
        myThread = threading.currentThread()
        try:
            myThread.transaction.begin()
            self.databaseWork()
            myThread.transaction.commit()
        except:
            myThread.transaction.rollback()
            raise
