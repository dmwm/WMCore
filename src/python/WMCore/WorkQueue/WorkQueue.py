import time
from sets import Set
from WMCore.DataStructs.WMObject import WMObject

class _WQElement(WMObject):
    """
    _WQElement_

    WQElement container

    """
    def __init__(self, specUrl = None, primaryBlock=None, blockLocations={}, priority = 0, 
                 online = 0, njobs = 0, whiteList=[], blackList=[]):
        WMObject.__init__(self)
        self.specUrl = specUrl
        # blockLocations is dict format of {blockName:[location List], ...}
        self.blockLocations = blockLocations
        #self.locations = locations
        self.primaryBlock = primaryBlock
        self.whiteList = whiteList
        self.blackList = blackList
        self.priority = priority
        self.online = online
        self.njobs = njobs
        self.time = time.time()
        self.status = "Available"
        self.wmSpec = WorkSpecParser(specUrl).getwmSpec()

    def __cmp__(x, y):
        tfactor = x.time
        current = time.time()
        weight = 0.01
        return (x.priority + weight*(current - x.time)) - (y.priority + weight*(current - y.time))
        
    def _sites(self):
        return [] #sites that contain all blocks
    
    def _online(self):
        return True #are all blocks staged?
    
    def match(self, conditions):
        """
        Function to match a unit of work to a condition
        
        Take list of current conditions and return the best match,
          or None if no match found
        """
        #TODO: Match against conditions dict, and return updated dict with taken resources removed
        if not _online:
            return None
        #For now just return first matching requirement 
        # Later add some ranking so most restrictive requirements match first
        for condition in conditions:
            site = conditions['Site']
            
            if site in self._sites:
                return condition
        return None 
    
    def getLocations(self):
        """
        _getLocations_
        
        return list of locations in the WQElement only if there is some common location
        in a primary block and its parent block
        """
        commonLocation = Set(self.blockLocations[self.primaryBlock])
        for locations in self.blockLocations.values():
            commonLocation = commonLocation.interSection(Set(locations))
        return list(commonLocation)
    
            
    def updateLocations(self, dbsHelper):
        """
        _updateLocations_
        
        update all the block locations in the WQElement 
        """
        for block in self.blockLocations.keys():
            locations = dbsHelper.getBlockLocations(block)
            # just replace the location since the previous location might be
            # deleted 
            self.blockLocations[block] = locations    
            
class WorkQueue(WMObject):
    """
    _WorkQueue_
    
    collection of work queue elements, 
    
    This  provide API for JSM (WorkQueuePool) - getWork(), gotWork()
    and injector
    """
    def __init__(self, dbsUrl):
        self.elements = [ ]
        self.dbsHelper = DBSHelper(dbsUrl)

    def addElement(self, specUrl = None, primaryBlock = None, parentBlocks=[], priority = 0, 
                   whiteList, blackList):
        """
        _addElement_
        
        TODO: eventually this will be the database update for WorkQueue table set
        """
        blockLocations = {} # TODO: Should be automated contains both primaryBlock and parentBlock 
        online = 1 # TODO: Should be automated 
        newElem = _WQElement(specUrl, primaryBlock, blockLocations, priority, online, njobs, 
                             whiteList, blackList)
        self.elements.add[newElem] 

    def match(self, conditions):
        """
        Loop over internal queue in priority order, 
          matching WQElements to resources available
        """
        results = []
        self.reorderList()
        for element in self.elements:
            #Act of matching returns the remaining match attributes 
            matched = element.match(conditions)
            if matched:
                results.append(element)
                conditions.pop(matched) #Remove used resource from further matches
        

    def reorderList(self):
        self.elements.sort()    
    
    def setStatus(self, subscription, status):
        """
        _setStatus_
        """
        # set the status of give subscription and status
        pass
    
    def setPriority(self, idnumber, newpriority):
        found = 0
        count = 0
        while (len(self.elements) < count or found):
            if (self.elements[count].idnumber == idnumber):
                found=1
                self.elements[count].priority = newpriority
            if (not found):
                print "Element not found nothing changed"
            else:
                self.ReorderList
                
    def updateLocationInfo(self):
        
        for element in self.elements:
            element.updateLocations(self.dbsHelper)
            
    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        """
        # update the location information in WorkQueue
        # if this is too much overhead use separate component
        self.uplateLocationInfo()
        subscriptions = []
        #for site in siteJobs.key():
        # might just return one  block
        blocks = self.match(siteJobs)
        # create fileset workflow and subscription
        #generate fileset name from multiple blocks
        #generate workflow name from multiple blocks
        filesetName = "Fileset"
        workflowName = "Workflow"
        wmbsHelper = WMBSHelper(self.wmSpec)
        subscriptions = wmbsHelper.createSubscription(fileName=workflowName,
                                                workflowName=workflowName)
        return subscriptions
    
    def gotWork(self, subscription):
        """
        _gotWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        try:
            self.setStatus(subscription, "Acquired")
            return True
        except:
            return False
    
    def doneWork(self, subscription):
        """
        _doneWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        try:
            self.setStatus(subscription, "Done")
            return True
        except:
            return False


    def queueWork(self, wmspec):
        """
        Take and queue work from a WMSpec
        """
        spec = workSpecParser(wmspec)
        for name, blocks, jobs in spec:
            self.addElement(specUrl = wmspec,
                            primaryBlock = name,
                            parentBlocks = [],
                            priority = workSpecParser.priority(),
                            whitelist = workSpecParser.whitelist(),
                            blacklist = workSpecParser.blacklist()
                            )
