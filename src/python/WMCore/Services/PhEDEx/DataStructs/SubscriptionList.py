"""
_SubscriptionList_

Module with data structures to handle PhEDEx subscriptions
in bulk.
"""
import logging

from WMCore.WMException import WMException

PhEDEx_VALID_SUBSCRIPTION_PRIORITIES = ['low', 'normal', 'high', 'reserved']


class PhEDExSubscriptionException(WMException):
    """
    _PhEDExSubscriptionException_

    Exception class for the phedex subscription
    """
    pass

class PhEDExSubscription(object):
    """
    _PhEDExSubscription_

    Data structure which contains PHEDEx fields for
    PhEDEx subscription data service
    """
    def __init__(self, datasetPathList, nodeList, group, level = 'dataset',
                 priority = 'normal', move = 'n', static = 'n', custodial = 'n',
                 request_only = 'y', blocks = None, subscriptionId = -1, comments=""):
        """
        Initialize PhEDEx subscription with default value
        """
        if isinstance(datasetPathList, basestring):
            datasetPathList = [datasetPathList]
        if isinstance(nodeList, basestring):
            nodeList = [nodeList]

        self.datasetPaths = set(datasetPathList)
        self.nodes = set(nodeList)

        self.level = level.lower()
        self.priority = priority.lower()
        self.move = move.lower()
        self.static = static.lower()
        self.group = group
        self.custodial = custodial.lower()
        self.request_only = request_only.lower()
        self.requesterID = None
        self.status = "New"
        self.comments = comments

        # Subscription id for internal accounting
        self.subscriptionIds = set([subscriptionId])

        # Optional blocks for non-dataset subscriptions
        self.blocks = blocks

        try:
            # Validation checks on the subscription
            for option in (self.static, self.custodial, self.request_only, self.move):
                assert option in ('y', 'n')
            assert self.priority in PhEDEx_VALID_SUBSCRIPTION_PRIORITIES
            assert self.level in ('dataset', 'block')
            if self.level == 'block':
                assert self.blocks is not None
        except AssertionError:
            msg = "The subscription is not a valid PhEDEx subscription.\n"
            msg += "Check the options for this subscription: \n"
            msg += "level: %s\n" % self.level
            msg += "priority: %s\n" % self.priority
            msg += "static: %s\n" % self.static
            msg += "move: %s\n" % self.move
            msg += "custodial: %s\n" % self.custodial
            msg += "blocks: %s\n" % str(self.blocks)
            raise PhEDExSubscriptionException(msg)

    def __str__(self):
        """
        Write out useful information for this object
        :return:
        """
        res = {'datasetPaths': self.datasetPaths, 'nodes': self.nodes,
               'priority': self.priority, 'move': self.move,
               'group': self.group, 'custodial': self.custodial,
               'request_only': self.request_only, 'blocks': self.blocks}
        return str(res)

    def isEqualOptions(self, subscription):
        return (self.level == subscription.level
                and self.priority == subscription.priority
                and self.request_only == subscription.request_only
                and self.custodial == subscription.custodial
                and self.group == subscription.group
                and self.move == subscription.move
                and self.static == subscription.static)

    def isEqualDatasetPaths(self, subscription):
        return (self.datasetPaths == subscription.datasetPaths
                and self.isEqualOptions(subscription))

    def isEqualNode(self, subscription):
        return (self.nodes == subscription.nodes
                and self.isEqualOptions(subscription))

    def addDatasetPaths(self, subscription):
        if self.requesterID != None:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ % (self.requesterID)
            raise Exception(msg)

        self.datasetPaths = self.datasetPaths.union(subscription.datasetPaths)
        self.subscriptionIds = self.subscriptionIds.union(subscription.subscriptionIds)

    def addNodes(self, subscription):
        if self.requesterID != None:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ % (self.requesterID)
            raise Exception(msg)

        self.nodes = self.nodes.union(subscription.nodes)
        self.subscriptionIds = self.subscriptionIds.union(subscription.subscriptionIds)

    def getDatasetPaths(self):
        return list(self.datasetPaths)

    def getSubscriptionIds(self):
        return list(self.subscriptionIds)

    def getDatasetsAndBlocks(self):
        """
        _getDatasetsAndBlocks_

        Get the block structure
        with datasets and blocks
        """
        return self.blocks

    def getBlocks(self):
        """
        _getBlocks_

        Return only the list of blocks
        """
        blocks = []
        for dset, listBlocks in self.blocks.items():
            blocks.extend(listBlocks)
        return blocks

    def getNodes(self):
        return list(self.nodes)

    def getRequesterID(self):
        return self.requesterID

    def setRequesterID(self, requesterId):

        if self.requesterID == None:
            self.requesterID = requesterId
        else:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ % (self.requesterID)
            raise Exception(msg)

    def matchesExistingTransferRequest(self, phedexDataSvc):
        """
        _matchesExistingTransferRequest_

        Check the given phedex data service to verify if an unapproved
        transfer request equal to this subscription is already in the system.
        """
        if len(self.datasetPaths) != 1 or len(self.nodes) != 1:
            msg = "matchesExistingTransferRequest can only run in single node/dataset subscriptions"
            raise PhEDExSubscriptionException(msg)
        if self.level != 'dataset':
            msg = "matchesExistingTransferRequest is only supported by dataset subscriptions"
            raise PhEDExSubscriptionException(msg)

        node = next(iter(self.nodes))
        dataset = next(iter(self.datasetPaths))
        # Get the unapproved requests involving the node and dataset in this subscription
        existingRequests = phedexDataSvc.getRequestList(dataset = dataset,
                                                        node = node,
                                                        decision = 'pending')['phedex']['request']
        for request in existingRequests:
            # Get the detailed information in the request
            requestId = request['id']
            requestInfo = phedexDataSvc.getTransferRequests(request = requestId)['phedex']['request']
            if not requestInfo:
                logging.error("Transfer request %s doesn't exist in PhEDEx", requestId)
                continue # Strange, but let it go.
            requestInfo = requestInfo[0] # It's a singleton
            # Make sure that the node is in the destinations
            destinations = requestInfo['destinations']['node']
            for nodeInfo in destinations:
                if nodeInfo['name'] == node:
                    break
            else:
                continue
            # Create a subscription with this info
            phedexRequest = PhEDExSubscription(self.datasetPaths, self.nodes,
                                               self.group, self.level, requestInfo['priority'],
                                               requestInfo['move'], requestInfo['static'],
                                               requestInfo['custodial'], self.request_only)
            if self.isEqualOptions(phedexRequest):
                return True

        return False

    def matchesExistingSubscription(self, phedexDataSvc):
        """
        _matchesExistingSubscription_

        Check the given phedex data service to verify if a PhEDEx subscription
        equal to this subscription is already in the system.
        """
        if len(self.datasetPaths) != 1 or len(self.nodes) != 1:
            msg = "matchesExistingSubscription can only run in single node/dataset subscriptions"
            raise PhEDExSubscriptionException(msg)
        if self.level != 'dataset':
            msg = "matchesExistingSubscription is only supported by dataset subscriptions"
            raise PhEDExSubscriptionException(msg)

        node = next(iter(self.nodes))
        dataset = next(iter(self.datasetPaths))
        # Check if the dataset has a subscription the given node
        existingSubscription = phedexDataSvc.subscriptions(dataset = dataset,
                                                           node = node)['phedex']['dataset']
        if len(existingSubscription) < 1:
            # No subscriptions
            return False
        datasetInfo = existingSubscription[0]
        for subscriptionInfo in datasetInfo['subscription']:
            # Check that the node in the subscription matches the current node
            if node != subscriptionInfo['node']:
                continue
            # Create a subscription with the info
            phedexSub = PhEDExSubscription(self.datasetPaths, self.nodes,
                                           self.group, subscriptionInfo['level'],
                                           subscriptionInfo['priority'], subscriptionInfo['move'],
                                           self.static, subscriptionInfo['custodial'],
                                           self.request_only)
            if self.isEqualOptions(phedexSub):
                return True

        return False

class SubscriptionList(object):
    """
    _SubscriptionList_

    Class represents collection of subscription.
    This organizes the subscriptions in a way to minimize their number.
    """
    def __init__(self):
        self._subList = []

    def addSubscription(self, subObj):
        """
        _addSubscription_
        Add a new subscription to the subscription policy.
        If the same subscription key exist just add the node list
        """

        for subscription in self._subList:
            if subscription.isEqualOptions(subObj):
                if subscription.isEqualNode(subObj):
                    subscription.addDatasetPaths(subObj)
                    return

        self._subList.append(subObj)

        return

    def compact(self):
        """
        _compact_

        Compact the subscription list by aggregating the subscriptions where the nodes
        share a list of dataset paths.
        """
        # Bag the subscriptions, keep indexes of bagged items to
        # avoid modifying the list in place or copying the list
        bags = []
        baggedIndexes = set()
        for i, subscriptionA in enumerate(self._subList):
            if i in baggedIndexes:
                continue
            bags.append([subscriptionA])
            for j, subscriptionB in enumerate(self._subList[i + 1:], i + 1):
                if j in baggedIndexes:
                    continue
                if subscriptionA.isEqualOptions(subscriptionB) and \
                    subscriptionA.isEqualDatasetPaths(subscriptionB):
                    bags[-1].append(subscriptionB)
                    baggedIndexes.add(j)

        # Aggregate the subscriptions in the bags
        newSubList = []
        for bag in bags:
            anchorSubscription = bag[0]
            for subscription in bag[1:]:
                anchorSubscription.addNodes(subscription)
            newSubList.append(anchorSubscription)
        self._subList = newSubList

    def getSubscriptionList(self):
        return self._subList
