import logging

class PhEDExSubscription(object):
    """
    _PhEDExSubscription_
    
    data structure which contains PHEDEx fields for 
    PhEDEx subscription data service 
    """
    def __init__(self, datasetPathList, nodeList, group,
                 level='dataset', priority='normal', move='n', static='n', 
                 custodial='n', requestOnly='y'):
        """
        initialize PhEDEx subscription with default value
        """
        if type(datasetPathList) == str:
            datasetPathList = [datasetPathList]
        if type(nodeList) == str:
            nodeList = [nodeList]
            
        self.datasetPaths = frozenset(datasetPathList)
        self.nodes = frozenset(nodeList)
        
        self.level = level.lower()
        # tier0 specific default
        self.priority = priority.lower() #subscription priority, 
        # either 'high', 'normal', or 'low'. Default is 'low' in PhEDEx

        # To check: move subscription should be 'y'
        # but automatic deletion should be turned off
        self.move = move.lower()
        # make growing subscription
        self.static = static.lower()
        self.requesterID = None
        self.status = "New"
        self.group = group
        self.custodial = custodial.lower() # 'y' or 'n', if 'y' then create the request but 
        # do not approve.  Default is 'n' in PhEDEx
        self.request_only = requestOnly.lower() #'y' or 'n', if 'y' then create the request 
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
                  """ % (self.requesterID)
            raise Exception, msg
        
        self.datasetPaths = self.datasetPaths.union(subscription.datasetPaths)
    
    def addNodes(self, subscription):
        if self.requesterID != None:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ % (self.requesterID)
            raise Exception, msg
        
        self.nodes = self.nodes.union(subscription.nodes)
    
    def getDatasetPaths(self):
        return list(self.datasetPaths)
    
    def getNodes(self):
        return list(self.nodes)
    
    def getRequesterID(self):
        return self.requesterID
    
    def setRequesterID(self, id):
        
        if self.requesterID == None:
            self.requesterID = id
        else:
            msg = """ PhEDEx subscription is already made with id: %s\n
                      Create a new subscription
                  """ % (self.requesterID)
            raise Exception, msg
        
             
class SubscriptionList(object):
    """
    _SubscriptionPolicy_
    
    class represents collection of subscription.
    This organize the subscription in a way to minimize the number of PhEDEx Subscription made
    Currently it will be organized by node.
    TODO: add more smarter way to organize subscription 
    """
    def __init__(self):
        self._subList = []
        
    def addSubscription(self, subObj):
        """
        _addSubscription_
        add a new subscription to the subscription policy.
        If the same subscription key exist just add the node list
        
        optimized when each node takes more than one datasetPath
        To do: need to improve algorithm in other cases, sort by nodes
        or dataset paths 
        """
            
        for subscription in self._subList:
            if subscription.isEqualOptions(subObj):
                if subscription.isEqualNode(subObj):
                    subscription.addDatasetPaths(subObj)
                    return
        
        self._subList.append(subObj)
        
        return
    
    def getSubscriptionList(self):
        return self._subList

if __name__ == "__main__":
    policy = SubscriptionList()
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
