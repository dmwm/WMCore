#!/usr/bin/env python
#pylint: disable-msg=W0613
"""
The actual subscription creation algorithm
"""
__all__ = []
__revision__ = "$Id: WorkflowManagerPoller.py,v 1.6 \
            2009/07/27 23:21:44 riahi Exp $"
__version__ = "$Revision: 1.8 $"

import threading
import logging
import re
import time

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
    def __init__(self):
        """
        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.queries = None
    
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
        availableWorkflows = self.queries.getUnsubscribedWorkflows() 
        logging.debug("Found %s unsubscribed managed workflows" \
              % len(availableWorkflows))

        
        # Get the details of all unsubscribed filesets
        availableFilesets = self.queries.getAllFilesets()

        logging.debug("Found %s filesets" % len(availableFilesets))
            
        # Match filesets to managed workflows  
        for managedWorkflow in availableWorkflows:
            # Workflow object cache to pass into Subscription constructor
            wfObj = None
            
            for fileset in availableFilesets:
                # Fileset object cache
                fsObj = None

                # Load the location information
                #whitelist = Set()
                #blacklist = Set()
                # Location is only caf  
                #locations = self.queries.getLocations(managedWorkflow['id'])
                #for location in locations:
                #    if bool(int(location['valid'])) == True:
                #        whitelist.add(location['site_name'])
                #    else:
                #        blacklist.add(location['site_name'])
                
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
                                     #whitelist = whitelist, \
                                     #blacklist = blacklist, \
                                     split_algo = managedWorkflow['split_algo'],
                                     type = managedWorkflow['type'])
                    newSub.create()

        time.sleep(10)

        managedWorkflows = self.queries.getManagedWorkflows()
        logging.debug("Found %s  managed workflows" \
              % len(managedWorkflows))

        unsubscribedFilesets = self.queries.getUnsubscribedFilesets()
        logging.debug("Found %s unsubscribed filesets" % \
                len(unsubscribedFilesets))

        for unsubscribedFileset in unsubscribedFilesets:
            # Workflow object cache to pass into Subscription constructor
            # FIXME
            wfObj = None

            for managedWork in managedWorkflows:

                logging.debug("The workflow %s" %managedWork['workflow'])

                # Fileset object cache
                wfObj = None
                fsObj = None

                # Load the location information
                #whitelist = Set()
                #blacklist = Set()
                # Location is only caf  
                #locations = self.queries.getLocations(managedWorkflow['id'])
                #for location in locations:
                #    if bool(int(location['valid'])) == True:
                #        whitelist.add(location['site_name'])
                #    else:
                #        blacklist.add(location['site_name'])

                # Attempt to match workflows to filesets
                if re.match(managedWork['fileset_match'], \
                     unsubscribedFileset['name']):
                    # Log in debug
                    msg = "Creating subscription for %s to workflow id %s"
                    msg %= (unsubscribedFileset['name'], \
                          managedWork['workflow'])
                    logging.debug(msg)

                    # Match found - Load the fileset if not already loaded
                    if not fsObj:
                        fsObj = Fileset(id = unsubscribedFileset['id'])
                        fsObj.load()

                    # Load the workflow if not already loaded
                    if not wfObj:
                        wfObj = Workflow(id = managedWork['workflow'])
                        wfObj.load()

                    # Create the subscription
                    newSub = Subscription(fileset = fsObj, \
                                     workflow = wfObj, \
                                     #whitelist = whitelist, \
                                     #blacklist = blacklist, \
                                     split_algo = managedWork['split_algo'],
                                     type = managedWork['type'])
                    newSub.create()
                    newSub.load()


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






 


