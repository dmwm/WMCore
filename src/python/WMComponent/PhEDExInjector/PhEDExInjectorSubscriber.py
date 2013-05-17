#!/usr/bin/env python
"""
_PhEDExInjectorSubscriber_

Poll the DBSBuffer database for unsubscribed datasets, and make subscriptions
associated with these datasets.

The subscription information is stored in the DBSBuffer subscriptions table and specifies the following options
for each dataset:

- site: Site to subscribe the data to
- custodial: 1 if the subscription must be custodial, non custodial otherwise
- auto_approve: 1 if the subscription should be approved automatically, request-only otherwise
- priority: Priority of the subscription, can be Low, Normal or High
- move: 1 if the subscription is a move subscription, 0 otherwise

The usual flow of operation is:

- Find unsuscribed datasets (i.e. dbsbuffer_dataset_subscription.subscribed = 0)
- Check for existing subscription in PhEDEx for such datasets, with the same
  configuration options as registered in the dataset, mark these as already subscribed
- Subscribe the unsubscribed datasets and mark them as such in the database,
  this is done according to the configuration options and aggregated to minimize
  the number of PhEDEx requests.

There is a safe mode operation, which is aimed for the Tier-0 use case:

Note: This only applies for datasets marked for custodial move subscriptions,
      in safe mode all other subscription flavors are treated as above

- Find all unsubscribed datasets
- Find all closed blocks belonging to unsubscribed datasets
  which don't contain files produced by active workflows, where
  an active workflow is defined as a workflow still present in WMBS.
- Subscribe those blocks as move custodial
- Don't mark the dataset as subscribed

The safe mode is activated using config.PhEDExInjector.safeMode = True

Additional options are:

- config.PhEDExInjector.subscribeDatasets, if False then this worker doesn't run
"""

import threading
import logging

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread

from WMCore.Services.PhEDEx import XMLDrop
from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.PhEDEx.DataStructs.SubscriptionList import PhEDExSubscription, SubscriptionList
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

from WMCore.DAOFactory import DAOFactory

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
        self.safeMode = getattr(config.PhEDExInjector, "safeMode", False)

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
        self.getUnsubscribedBlocks = daofactory(classname = "GetUnsubscribedBlocks")
        self.markSubscribed = daofactory(classname = "MarkDatasetSubscribed")

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

        Run the subscription algorithm as configured
        """
        if self.safeMode:
            self.subscribeBlocks()
        self.subscribeDatasets()
        return

    def subscribeBlocks(self):
        """
        _subscribeBlocks_

        Subscribe custodial blocks that are requested
        as a move subscription, only on non-active workflows.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Check for candidate blocks for subscription
        blocksToSubscribe = self.getUnsubscribedBlocks.execute(conn = myThread.transaction.conn,
                                                               transaction = True)

        # TODO: Check for existing block subscriptions

        # Sort by dataset and subscription options
        # The subscription map has tuples as keys where the tuple
        # has the following structure
        # (site<TX_XX_XX_XX>, custodial<y or n>, request_only<y or n>, move<y or n>, priority<low,normal,high>)
        subscriptionMap = {}
        for subInfo in blocksToSubscribe:
            key = (subInfo['site'], subInfo['custodial'], subInfo['request_only'],
                   subInfo['move'], subInfo['priority'])
            if key not in subscriptionMap:
                subscriptionMap[key] = {}
            if subInfo['path'] not in subscriptionMap[key]:
                subscriptionMap[key][subInfo['path']] = []
            subscriptionMap[key][subInfo['path']].append(subInfo['blockname'])

        for subOptions in subscriptionMap:
            # Check that the site is valid
            site = subOptions[0]
            custodial = subOptions[1]
            request_only = subOptions[2]
            move = subOptions[3]
            priority = subOptions[4]
            if site not in self.cmsToPhedexMap:
                msg = "Site %s doesn't appear to be valid to PhEDEx, " % site
                msg += "skipping subscriptions for datasets: %s" % ', '.join(x for x in subscriptionMap[subOptions])
                logging.error(msg)
                self.sendAlert(7, msg = msg)
                continue
            datasets = subscriptionMap[subOptions].keys()

            # Get the phedex node
            isMSS = "MSS" in self.cmsToPhedexMap[site]
            phedexNode = self.cmsToPhedexMap[site].get("MSS") \
                            or self.cmsToPhedexMap[site]["Disk"]
            logging.info("Subscribing %s to %s" % (datasets, site))

            # Avoid custodial subscriptions to disk nodes
            if not isMSS and custodial == 'y': custodial = 'n'
            # Avoid move subscriptions and replica
            if custodial == 'n': move = 'n'

            logging.info("Request options: Custodial - %s, Move - %s" % (custodial.upper(),
                                                                         move.upper(),))

            newSubscription = PhEDExSubscription(datasets, phedexNode, self.group,
                                                 custodial = custodial,
                                                 request_only = request_only,
                                                 move = move,
                                                 priority = priority,
                                                 level = 'block',
                                                 blocks = subscriptionMap[subOptions])
            try:
                xmlData = XMLDrop.makePhEDExXMLForBlocks(self.dbsUrl,
                                                         newSubscription.getDatasetsAndBlocks())
                logging.debug(str(xmlData))
                self.phedex.subscribe(newSubscription, xmlData)
            except Exception, ex:
                logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
                logging.error("Exception: %s" % str(ex))

    def subscribeDatasets(self):
        """
        _subscribeDatasets_

        Poll the database for datasets and subscribe them.
        """
        myThread = threading.currentThread()
        myThread.transaction.begin()

        # Check for completely unsubscribed datasets
        unsubscribedDatasets = self.getUnsubscribed.execute(safeMode = self.safeMode,
                                                            conn = myThread.transaction.conn,
                                                            transaction = True)

        # Keep a list of subscriptions to tick as subscribed in the database
        subscriptionsMade = []

        # Create a list of subscriptions as defined by the PhEDEx data structures
        subs = SubscriptionList()

        # Create the subscription objects and add them to the list
        # The list takes care of the sorting internally
        for subInfo in unsubscribedDatasets:
            site = subInfo['site']

            # Get the phedex node
            if site not in self.cmsToPhedexMap:
                msg = "Site %s doesn't appear to be valid to PhEDEx, " % site
                msg += "skipping subscription: %s" % subInfo['id']
                logging.error(msg)
                self.sendAlert(7, msg = msg)
                continue
            isMSS = "MSS" in self.cmsToPhedexMap[site]
            phedexNode = self.cmsToPhedexMap[site].get("MSS") \
                            or self.cmsToPhedexMap[site]["Disk"]

            # Avoid custodial subscriptions to disk nodes
            if not isMSS: subInfo['custodial'] = 'n'
            # Avoid move subscriptions and replica
            if subInfo['custodial'] == 'n': subInfo['move'] = 'n'

            phedexSub = PhEDExSubscription(subInfo['path'], phedexNode,
                                           self.group, priority = subInfo['priority'],
                                           move = subInfo['move'], custodial = subInfo['custodial'],
                                           request_only = subInfo['request_only'], subscriptionId = subInfo['id'])

            # Check if the subscription is a duplicate
            if phedexSub.matchesExistingSubscription(self.phedex) or \
                phedexSub.matchesExistingTransferRequest(self.phedex):
                subscriptionsMade.append(subInfo['id'])
                continue

            # Add it to the list
            subs.addSubscription(phedexSub)

        # Compact the subscriptions
        subs.compact()

        for subscription in subs.getSubscriptionList():
            try:
                xmlData = XMLDrop.makePhEDExXMLForDatasets(self.dbsUrl,
                                                           subscription.getDatasetPaths())
                logging.debug(str(xmlData))
                msg = "Subscribing: %s to %s, with options: " % (subscription.getDatasetPaths(), subscription.getNodes())
                msg += "Move: %s, Custodial: %s, Request Only: %s" % (subscription.move, subscription.custodial, subscription.request_only)
                logging.info(msg)
                self.phedex.subscribe(subscription, xmlData)
            except Exception, ex:
                logging.error("Something went wrong when communicating with PhEDEx, will try again later.")
                logging.error("Exception: %s" % str(ex))
            else:
                subscriptionsMade.extend(subscription.getSubscriptionIds())

        # Register the result in DBSBuffer
        if subscriptionsMade:
            self.markSubscribed.execute(subscriptionsMade,
                                        conn = myThread.transaction.conn,
                                        transaction = True)

        myThread.transaction.commit()
        return
