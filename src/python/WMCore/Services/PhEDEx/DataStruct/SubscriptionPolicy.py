import logging

class PhEDExSubscription(object):
    """
    _PhEDExSubscription_
    
    data structure which contains PHEDEx fields for 
    PhEDEx subscription data service 
    """
    def __init__(self, datasetPathTuple, nodeTuple, priority='high', 
                 requestOnly='y'):
        """
        initialize PhEDEx subscription with default value
        """
        if type(datasetPathTuple) == tuple:
            datasetPathIDs = [datasetPathTuple[0]]
            datasetPaths = [datasetPathTuple[1]]
        else:
            raise TypeError, "First argument should be tuple (dataPathID, datasetPath)"
            
        if type(nodeTuple) == tuple:
            nodeIDs = [nodeTuple[0]]
            nodes = [nodeTuple[1]]
        else:
            raise TypeError, "First argument should be tuple (nodeID, node)"
            
        self.datasetPaths = frozenset(datasetPaths)
        self.datasetPathIDs = frozenset(datasetPathIDs)
        
        logging.debug("SubPolicy class dataset path %s" % self.datasetPaths)
        self.nodeIDs = frozenset(nodeIDs)
        self.nodes = frozenset(nodes)
        
        self.level = 'dataset'
        # tier0 specific default
        self.priority = priority.lower() #subscription priority, 
        # either 'high', 'normal', or 'low'. Default is 'low' in PhEDEx

        # To check: move subscription should be 'y'
        # but automatic deletion should be turned off
        self.move = 'n'
        # make growing subscription
        self.static = 'n'
        self.requesterID = None
        self.status = "New"
        self.group = "DataOps"
        self.custodial = 'y' # 'y' or 'n', if 'y' then create the request but 
        # do not approve.  Default is 'n' in PhEDEx
        self.request_only = requestOnly #'y' or 'n', if 'y' then create the request 
        # but do not approve.  Default is 'n' in PhEDEx
        
        
    def isEqualOptions(self, subscription):
        # currently only priority and request_only are configurable.
        # rest of them are hard coded
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
                  """ (self.requesterID)
            raise Exception, msg
        
        self.datasetPathIDs = \
                self.datasetPathIDs.union(subscription.datasetPathIDs)
        self.datasetPaths = self.datasetPaths.union(subscription.datasetPaths)
    
    def addNodes(self, subscription):
        if self.requesterID != None:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ (self.requesterID)
            raise Exception, msg
        
        self.nodeIDs = self.nodeIDs.union(subscription.nodeIDs)
        self.nodes = self.nodes.union(subscription.nodes)
    
    def getDatasetPathIDs(self):
        return list(self.datasetPathIDs)
    
    def getDatasetPaths(self):
        return list(self.datasetPaths)
    
    def getNodes(self):
        return list(self.nodes)
    
    def getNodeIDs(self):
        return list(self.nodeIDs)
    
    def getRequesterID(self):
        return self.requesterID
    
    def setRequesterID(self, id):
        
        if self.requesterID == None:
            self.requesterID = id
        else:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ (self.requesterID)
            raise Exception, msg
        
             
class SubscriptionPolicy(object):
    """
    _SubscriptionPolicy_
    
    class represents collection of subscription.
    """
    def __init__(self):
        self.subscriptionList = []
        
    def addSubscription(self, subObj):
        """
        _addSubscription_
        add a new subscription to the subscription policy.
        If the same subscription key exist just add the node list
        
        optimized when each node takes more than one datasetPath
        To do: need to improve algorithm in other cases, sort by nodes
        or dataset paths 
        """
            
        for subscription in self.subscriptionList:
            if subscription.isEqualOptions(subObj):
                if subscription.isEqualNode(subObj):
                    subscription.addDatasetPaths(subObj)
                    return
        
        self.subscriptionList.append(subObj)
        
        return
    
    def getSubscriptionList(self):
        return self.subscriptionList

if __name__ == "__main__":
    policy = SubscriptionPolicy()
    # what will you do with run ID.
    row = [6, "/Cosmics/Test-CRAFTv8/RAW",1, "T2_CH_CAF" , 'high', 'y']
    results = []
    for i in range(6):
        results.append(row)
    
    results.append([7, "/Cosmics/Test-CRAFTv8/ALCARECO", 2, "FNAL" , 'normal', 'y'])
    results.append([8, "/Cosmics/Test-CRAFTv8/RECO", 1, "T2_CH_CAF" , 'high', 'y'])
    results.append([8, "/Cosmics/Test-CRAFTv8/RECO", 2, "FNAL" , 'high', 'y'])
    
    print policy.getSubscriptionList()
            
    for row in results:
        # make a tuple (dataset_path_id, dataset_path_name)
        # make a tuple (node_id, node_name)
        subscription = PhEDExSubscription((row[0], row[1]), (row[2], row[3]),
                                          row[4], row[5])
        policy.addSubscription(subscription)
    
    i = 0    
    for sub in policy.getSubscriptionList():
        i += 1
        print "Subscription %s" % i
        print sub.getDatasetPaths()
        print sub.getNodes()
        print    