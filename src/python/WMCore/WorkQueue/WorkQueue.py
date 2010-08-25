from WMCore.DataStructs.WMObject import WMObject
import time

class WQElement(WMObject):
    """
    _WQElement_

    WQElement container

    """
    def __init__(self, idnumber = None, blocks = [ ], locations = [ ], priority = 0, 
                 online = 0, njobs = 0, whiteList=[], blackList=[]):
        WMObject.__init__(self)
        self.idnumber = idnumber
        self.blocks = blocks
        #self.locations = locations
        self.block = block
        self.whiteList = whiteList
        self.blackList = blackList
        self.priority = priority
        self.online = online
        self.njobs = njobs
        self.time = time.time()
        self.status = "Available"

    def __cmp__(x, y):
        tfactor = x.time
        current = time.time()
        weight = 0.01
        return (x.priority + weight*(current - x.time)) - (y.priority + weight*(current - y.time))
        

    def match(self, conditions):
        """
        Function to match a unit of work to a condition
        
        Take list of current conditions and return the best match,
          or None if no match found
        """
        if not _online:
            return None
        #For now just return first matching requirement 
        # Later add some ranking so most restrictive requirements match first
        for condition in conditions:
            site = conditions['Site']
            
            if site in self._sites:
                return condition
        return None 
        
    def _sites(self):
        return [] #sites that contain all blocks
    
    def _online(self):
        return True #are all blocks staged?
        
class WorkQueue(WMObject):
    def __init__(self):
        self.elements = [ ]

    def AddElement(self, idnumber = None, blocks = [ ],  priority = 0,  njobs = 0):
        locations = [ ] # Should be automated 
        online = 1 # Should be automated 
        x = WQElement(idnumber, blocks, locations, priority, online, njobs)
        self.elements = self.elements + [ x ] 

    def match(self, conditions):
        """
        Loop over internal queue, matching WQElements to resources available
        """
        results = []
        self.reOrderList()
        for element in self.elements:
            #Act of mathcing returns the matched requirement
            matched = element.match(conditions)
            if matched:
                results.append(element)
                conditions.pop(matched) #Remove used resource from further matches
        

    def ReorderList(self):
        self.elements.sort()    
    
    def setStatus(self, subscription, status):
        """
        _setStatus_
        
        """
        # set the status of give subscription and status
        pass
    
    def SetPriority(self, idnumber, newpriority):
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
                
    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        """
        
        for site in siteJobs.key():
            # might just return one  block
            blocks = self.getTopPriorityBlocks(site, siteJobs[site])
            # create fileset workflow and subscription
            for block in blocks:
                WMBSHelper.createFileset(block)
            
    
    def gotWork(self, subscription):
        """
        _gotWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        self.
        

