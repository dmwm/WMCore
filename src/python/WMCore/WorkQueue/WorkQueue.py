#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing
  
WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.
    
TODO: Persist WQElements
     
"""

import time
try:
    set
except ImportError:
    from sets import Set as set
from WMCore.DataStructs.WMObject import WMObject
from WMCore.WorkQueue.DBSHelper import DBSHelper
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser

class _WQElement(WMObject):
    """
    _WQElement_

    WQElement container

    """
    def __init__(self, specUrl, primaryBlock = None, blockLocations = None,
                 priority = 0, online = 0, njobs = 0, whiteList = None,
                 blackList = None):
        WMObject.__init__(self)
        self.specUrl = specUrl
        # blockLocations is dict format of {blockName:[location List], ...}
        self.blockLocations = blockLocations or {}
        #self.locations = locations
        self.primaryBlock = primaryBlock
        self.whiteList = whiteList or []
        self.blackList = blackList or []
        self.priority = priority
        self.online = online
        self.njobs = njobs
        self.time = time.time()
        self.status = "Available"
        self.subscription = None
        self.wmSpec = WorkSpecParser(specUrl)


    def __cmp__(self, y):
        """
        Compare work units so they can be sorted by priority
          priority determined by a weighted average of the 
            static priority and the queueing time
        """
        tfactor = self.time
        current = time.time()
        weight = 0.01
        return (self.priority + weight*(current - self.time)) - (y.priority + weight*(current - y.time))


    def _sites(self):
        """
        Return sites that contain all blocks
        """
        return []


    def _online(self):
        """
        Are all blocks online?
        """
        return True


    def match(self, conditions):
        """
        Function to match a unit of work to a condition
        
        Take list of current conditions and 
        returns tuple containing a bool and a dict.
        Boolean indicates if a match was made, the dict contains the
        original conditions passed in minus any that were used in the match
        """
        if not self._online():
            return False, conditions

        #TODO: Add some ranking so most restrictive requirements match first
        for site, slots in conditions.iteritems():
            #For now just return first matching requirement 
            #TODO: add ranking so most restrictive requirements match first
            for condition in conditions:
                site = condition['Site']
                
                #TODO: Refine slot calc,
                #Options: Strict matching?, x% of block slots available?
                #For now strict - assume block small enough to release
                if site in self._sites and slots > self.njobs:
                    newconditions = {}
                    newconditions.update(conditions)
                    newslots = slots - self.njobs
                    if newslots > 0:
                        newconditions[site] = newslots
                    else:
                        newconditions.pop(site)
                    return True, newconditions
        # if here - never matched anything
        return False, conditions


    def setStatus(self, status):
        """
        Change status to that given
        """
        self.status = status
        
    
    def getLocations(self):
        """
        _getLocations_
        
        return list of locations in the WQElement only if there is some common location
        in a primary block and its parent block
        """
        commonLocation = set(self.blockLocations[self.primaryBlock])
        for locations in self.blockLocations.values():
            commonLocation = commonLocation.intersection(set(locations))
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


    def addElement(self, specUrl, nJobs, dbsUrl, primaryBlock = None,
                   parentBlocks = None, priority = 0,
                   whiteList = None, blackList = None):
        """
        _addElement_
        
        TODO: eventually this will be the database update for WorkQueue table set
        """
        parentBlocks = parentBlocks or []
        dbsHelper = DBSHelper(dbsUrl)
        blockLocations = {primaryBlock:
                          dbsHelper.getBlockLocations(primaryBlock)} # TODO: Should be automated contains both primaryBlock and parentBlock 
        for block in parentBlocks:
            blockLocations[block] = dbsHelper.getBlockLocations(block)
            
        online = 1 # TODO: Should be automated 
        newElem = _WQElement(specUrl, primaryBlock, blockLocations, priority, online, 
                             nJobs, whiteList, blackList)
        self.elements.append(newElem) 


    def match(self, conditions):
        """
        Loop over internal queue in priority order, 
          matching WQElements to resources available
        """
        results = []
        self.reorderList()
        for element in self.elements:
            #Act of matching returns the remaining match attributes 
            matched, conditions = element.match(conditions)
            if matched:
                results.append(element)
                if not conditions:
                    break # stop matching when no resources left      
        return results
        
        return results

    def reorderList(self):
        self.elements.sort()    


    def setStatus(self, subscription, status):
        """
        _setStatus_
        """
        # set the status of give subscription and status
        results = []
        for element in self.elements:
            if element.subscription == subscription:
                element.setStatus(status)
                results.append(element)
        return results


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
                self.reorderList()
                

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
        self.updateLocationInfo()
        subscriptions = []
        #for site in siteJobs.key():
        # might just return one  block
        wqElementList = self.match(siteJobs)
        for wqElement in wqElementList:
            wmSpec = WorkSpecParser(wqElement.specUrl)
            wmbsHelper = WMBSHelper(wmSpec)
            # create fileset workflow and subscription
            # generate workflow name from wmSpec names
            workflowName= "Workflow"
            
            results = {}
            for site in wqElementList["sites"]: 
                # generate fileset name from multiple blocks using some convention.
                # fileset should be blocks processed in the same sites
                filesetName = "Fileset"
                subscription = wmbsHelper.createSubscription(fileName=workflowName,
                                                    	     workflowName=workflowName)
                results[site] = subscription
        return results

    
    def gotWork(self, subscription):
        """
        _gotWork_
        
        this is called by JSM
        update the WorkQueue status table and remove from further consideration
        """
        taken_blocks = self.setStatus(subscription, "Acquired")
        [self.elements.remove(x) for x in [taken_blocks]] 
        return True


    def doneWork(self, subscription):
        """
        _doneWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        self.setStatus(subscription, "Done")
        return True


    def queueWork(self, wmspec):
        """
        Take and queue work from a WMSpec
        """
        spec = WorkSpecParser(wmspec)
        for name, blocks, jobs in spec:
            self.addElement(specUrl = wmspec,
                            primaryBlock = name,
                            parentBlocks = [],
                            priority = spec.priority(),
                            whitelist = spec.siteWhitelist(),
                            blacklist = spec.siteBlacklist(),
                            nJobs = jobs,
                            dbsUrl = spec.wmSpec.dbsUrl)
        return True
