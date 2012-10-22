#!/usr/bin/env python
"""
_PhEDExInjectorSubscriber_

Poll the DBSBuffer database for unsubscribed datasets, and make subscriptions
according to the specs associated with this dataset.

Each dataset can be associated with multiple task, each task specifies (through the spec) the following options:

- CustodialSites: Sites where the dataset should be custodial
- NonCustodialSites: Sites where the dataset should be non-custodial
- AutoApproveSites: Sites where the subscription will be approved automatically with the request
- Priority: Priority of the subscription, can be Low, Normal or High

As different task can have different options, these are aggregated following this simple rules:

- All sites in the first three parameters will be included, removing duplicates. If a site is in the custodial list, it will be
  removed from the NonCustodial and AutoApprove lists. Any site in the AutoApprove list that is not in the non-custodial
  list will be ignored.
- The lowest configured priority will be chosen for each dataset

There are 2 modes of operation for the subscriber, defined by config.PhEDExInjector.safeOperationMode. Defaults to False

- "Unsafe" operation mode (False): All custodial subscriptions are Move subscriptions and
                                   are requested as soon as the unsuscribed dataset appears in the dbsbuffer table
                                   and there is at least one file in Global DBS and PhEDEx.
                                   All non-custodial subscriptions are Replica, and requested at the same time as the custodial ones.
- "Safe" operation mode (True): Custodial subscriptions are done in two phases, when a dataset appears in dbsbuffer
                                a Replica subscription will be requested. The dataset will be kept in a non-terminal
                                subscription state in dbsbuffer, it will be polled and when there is no associated
                                workflow in WMBS then a Move subscription will be requested.
                                Non-custodial subscriptions behave as in "Unsafe" mode.

Additional options are:

- config.PhEDExInjector.subscribeDatasets, if False then this component doesn't run
"""

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from WMCore.DAOFactory import DAOFactory

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

"""
Useful lambda functions
"""
# Add site lists without duplicates
extendWithoutDups = lambda x, y : x + list(set(y) - set(x))
# Choose the lowest priority
solvePrioConflicts = lambda x, y : y if x == "High" or y == "Low" else x

class PhEDExInjectorSubscriber(BaseWorkerThread):
    """
    _PhEDExInjectorSubscriber_

    Poll the DBSBuffer database and subscribe datasets as they are
    created.
    """
    def __init__(self, config):
        """
        ___init___

        Initialise class members
        """
        BaseWorkerThread.__init__(self)
        self.phedex = PhEDEx({"endpoint": config.PhEDExInjector.phedexurl}, "json")
        self.siteDB = SiteDBJSON()
        self.dbsUrl = config.DBSInterface.globalDBSUrl
        self.group = getattr(config.PhEDExInjector, "group", "DataOps")
        self.safeMode = getattr(config.PhEDExInjector, "safeOperationMode", False)

        # Subscribed state in the DBSBuffer table for datasets
        self.terminalSubscriptionState = 1
        if self.safeMode:
            self.terminalSubscriptionState = 2

        # We will map node names to CMS names, that what the spec will have.
        # If a CMS name is associated to many PhEDEx node then choose the MSS option
        self.cmsToPhedexMap = {}

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

        self.getUnsubscribed = daofactory(classname = "GetUnsubscribedDatasets")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")
        self.getPartiallySubscribed = daofactory(classname = "GetPartiallySubscribedDatasets")

        nodeMappings = self.phedex.getNodeMap()
        for node in nodeMappings["phedex"]["node"]:

            cmsName = self.siteDB.phEDExNodetocmsName(node["name"])

            if cmsName not in self.cmsToPhedexMap:
                self.cmsToPhedexMap[cmsName] = {}

            logging.info("Loaded PhEDEx node %s for site %s" % (node["name"], cmsName))
            if node["kind"] not in self.cmsToPhedexMap[cmsName]:
                self.cmsToPhedexMap[cmsName][node["kind"]] = node["name"]

        return

    def algorithm(self, parameters):
        """
        _algorithm_

        Poll the database for datasets and subscribe them.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Check for completely unsubscribed datasets
        unsubscribedDatasets = self.getUnsubscribed.execute(conn = myThread.transaction.conn,
                                                            transaction = True)

        if self.safeMode:
            partiallySubscribedDatasets = self.getPartiallySubscribed.execute(conn = myThread.transaction.conn,
                                                                              transaction = True)
            unsubscribedDatasets.extend(partiallySubscribedDatasets)
            partiallySubscribedSet = set()
            for entry in partiallySubscribedDatasets:
                partiallySubscribedSet.add(entry["path"])

        # Map the datasets to their specs
        specDatasetMap = {}
        for unsubscribedDataset in unsubscribedDatasets:
            datasetPath = unsubscribedDataset["path"]
            workflow = unsubscribedDataset["workflow"]
            spec = unsubscribedDataset["spec"]

            if datasetPath not in specDatasetMap:
                specDatasetMap[datasetPath] = []
            specDatasetMap[datasetPath].append({"workflow" : workflow, "spec" : spec})

        specCache = {}
        siteMap = {}
        # Distribute the subscriptions by site, type and priority
        # This is to make as few subscriptions as possible
        # Site map values are dictionaries where the keys are tuples (Prio, Custodial, AutoApprove, Move)
        # Where Custodial is boolean, Prio is in ["Low", "Normal", "High"], AutoApprove is boolean and Move is boolean
        for dataset in specDatasetMap:
            # Aggregate all the different subscription configurations
            subInfo = {}
            for entry in specDatasetMap[dataset]:
                if not entry["spec"]:
                    # Can't use this spec, there isn't one
                    continue
                # Load spec if not in the cache
                if entry["spec"] not in specCache:
                    helper = WMWorkloadHelper()
                    try:
                        helper.load(entry["spec"])
                        specCache[entry["spec"]] = helper
                    except Exception:
                        #Couldn't load it , alert and carry on
                        msg = "Couldn't load spec: %s" % entry["spec"]
                        logging.error(msg)
                        self.sendAlert(7, msg = msg)
                        continue
                #If we are running in safe mode, we need to know if the workflow is ready
                # We have the spec, get the info
                helper = specCache[entry["spec"]]
                workflowSubInfo = helper.getSubscriptionInformation()
                datasetSubInfo = workflowSubInfo.get(dataset, None)
                if datasetSubInfo and subInfo:
                    subInfo["CustodialSites"] = extendWithoutDups(subInfo["CustodialSites"], datasetSubInfo["CustodialSites"])
                    subInfo["NonCustodialSites"] = extendWithoutDups(subInfo["NonCustodialSites"], datasetSubInfo["NonCustodialSites"])
                    subInfo["AutoApproveSites"] = extendWithoutDups(subInfo["AutoApproveSites"], datasetSubInfo["AutoApproveSites"])
                    subInfo["Priority"] = solvePrioConflicts(subInfo["Priority"], datasetSubInfo["Priority"])
                elif datasetSubInfo:
                    subInfo = datasetSubInfo

            # We now have aggregated subscription information for this dataset in subInfo
            # Distribute it by site
            if not subInfo:
                #Nothing to do, log and continue
                msg = "No subscriptions configured for dataset %s" % dataset
                logging.warning(msg)
                self.markSubscribed.execute(dataset, subscribed = self.terminalSubscriptionState,
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                continue
            # Make sure that a site is not configured both as non custodial and custodial
            # Non-custodial is believed to be the right choice
            subInfo["CustodialSites"] = list(set(subInfo["CustodialSites"]) - set(subInfo["NonCustodialSites"]))
            for site in subInfo["CustodialSites"]:
                if site not in siteMap:
                    siteMap[site] = {}
                if self.safeMode and dataset not in partiallySubscribedSet:
                    tupleKey = (subInfo["Priority"], True, False, False)
                else:
                    tupleKey = (subInfo["Priority"], True, False, True)
                if tupleKey not in siteMap[site]:
                    siteMap[site][tupleKey] = []
                siteMap[site][tupleKey].append(dataset)

            # If we are in safe mode and this is a partially subscribed dataset,
            # then the non-custodial were done in a previous cycle
            if self.safeMode and dataset in partiallySubscribedSet:
                self.markSubscribed.execute(dataset, subscribed = self.terminalSubscriptionState,
                                            conn = myThread.transaction.conn,
                                            transaction = True)
                continue

            for site in subInfo["NonCustodialSites"]:
                if site not in siteMap:
                    siteMap[site] = {}
                autoApprove = False
                if site in subInfo["AutoApproveSites"]:
                    autoApprove = True
                tupleKey = (subInfo["Priority"], False, autoApprove)
                if tupleKey not in siteMap[site]:
                    siteMap[site][tupleKey] = []
                siteMap[site][tupleKey].append(dataset)

            self.markSubscribed.execute(dataset, subscribed = 1,
                                        conn = myThread.transaction.conn,
                                        transaction = True)

        # Actually request the subscriptions
        for site in siteMap:
            # Check that the site is valid
            if site not in self.cmsToPhedexMap:
                msg = "Site %s doesn't appear to be valid to PhEDEx" % site
                logging.error(msg)
                self.sendAlert(7, msg = msg)
                continue
            for subscriptionFlavor in siteMap[site]:
                datasets = siteMap[site][subscriptionFlavor]
                # Check that the site is valid
                if "MSS" in self.cmsToPhedexMap[site]:
                    phedexNode = self.cmsToPhedexMap[site]["MSS"]
                else:
                    phedexNode = self.cmsToPhedexMap[site]["Disk"]
                logging.info("Subscribing %s to %s" % (datasets, site))
                options = {"custodial" : "n", "requestOnly" : "y",
                           "priority" : subscriptionFlavor[0].lower(),
                           "move" : "n"}
                if subscriptionFlavor[1]:
                    options["custodial"] = "y"
                    if subscriptionFlavor[3]:
                        options["move"] = "y"
                if subscriptionFlavor[2]:
                    options["requestOnly"] = "n"

                newSubscription = PhEDExSubscription(datasets, phedexNode, self.group,
                                                     **options)

                xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl,
                                                           newSubscription.getDatasetPaths())
                logging.debug(str(xmlData))
                self.phedex.subscribe(newSubscription, xmlData)


        myThread.transaction.commit()
        return
