#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing
  
WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.
    
TODO: Persist WQElements
     
"""

import time
# pylint: disable-msg=W0104,W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg=W0104,W0622
from WMCore.DataStructs.WMObject import WMObject
from WMCore.WorkQueue.DBSHelper import DBSHelper
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
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
        self.primaryBlock = primaryBlock
        # blockLocations is dict format of {blockName:[location List], ...}
        self.blockLocations = blockLocations or {}
        if primaryBlock:
            blockLocations[primaryBlock] = []
        self.whiteList = whiteList or []
        self.blackList = blackList or []
        self.priority = priority
        self._online = online
        self.njobs = njobs
        self.time = time.time()
        self.status = "Available"
        self.subscription = None
        self.wmSpec = WorkSpecParser(specUrl)


    def __cmp__(self, other):
        """
        Compare work units so they can be sorted by priority
          priority determined by a weighted average of the 
            static priority and the queueing time
        """
        current = time.time()
        weight = 0.01
        lhs = self.priority + weight * (current - self.time)
        rhs = other.priority + weight * (current - other.time)
        return cmp(rhs, lhs) # Contrary to normal sorting (high priority first)


    def __str__(self):
        return "WQElement: %s:%s priority=%s, njobs=%s, time=%s" % (self.specUrl,
                                            self.primaryBlock, self.priority,
                                            self.njobs, self.time)


    def online(self):
        """
        Are all blocks online?
        """
        return True


    def dataAvailable(self, site):
        """
        Can we run at the given site?
        """
        if not self.blockLocations:
            return True # no input blocks - match any site
        #TODO: Stager behavior faked for now - should be site specific
        return self.online() and site in self.locations
    
    
    def slotsAvailable(self, slots):
        """
        Do we have enough slots available?
        """
        #Options: Strict matching?, x% of block slots available?
        #For now strict - assume block small enough to release
        return slots >= self.njobs


    def match(self, conditions):
        """
        Function to match a unit of work to a condition
        
        Take list of current conditions and 
        returns tuple containing a bool and a dict.
        Boolean indicates if a match was made, the dict contains the
        original conditions passed in minus any that were used in the match
        """
        if self.status != "Available":
            return False, conditions

        #TODO: Add some ranking so most restrictive requirements match first
        for site, slots in conditions.iteritems():
            if self.dataAvailable(site) and self.slotsAvailable(slots):
                newconditions = {}
                newconditions.update(conditions)
                newslots = slots - self.njobs
                if newslots > 0:
                    newconditions[site] = newslots
                else:
                    newconditions.pop(site)
                return True, newconditions

        return False, conditions


    def setStatus(self, status):
        """
        Change status to that given
        """
        self.status = status
        
    
    def locations(self):
        """
        _locations_
        
        return list of locations in the WQElement only if there is some common location
        in a primary block and its parent block
        """
        commonLocation = set(self.blockLocations[self.primaryBlock])
        for locations in self.blockLocations.values():
            commonLocation = commonLocation.intersection(set(locations))
        return list(commonLocation)
    locations = property(locations)
    
            
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
        self.dbsHelper = DBSReader(dbsUrl)


    def __len__(self):
        return len(self.elements)


    def addElement(self, specUrl, nJobs, primaryBlock = None, blocks = None,
                   priority = 0, whiteList = None, blackList = None):
        """
        _addElement_
        
        TODO: eventually this will be the database update for WorkQueue table set
        """
        blocks = blocks or []
        whiteList = whiteList or []
        blackList = blackList or []
        #Fill Block locations later
        blockLocations = {}
        for block in blocks:
            blockLocations[block] = []
#        if primaryBlock:
#            blockLocations[primaryBlock] = self.dbsHelper.listFileBlockLocation(primaryBlock)
#        for block in blocks:
#            blockLocations[block] = self.dbsHelper.listFileBlockLocation(block)
        online = True # TODO: Should be automated 
        element = _WQElement(specUrl, primaryBlock, blockLocations, 
                                        priority, online, nJobs,
                                        whiteList, blackList)
        self.elements.append(element)


    def match(self, conditions):
        """
        Loop over internal queue in priority order, 
          matching WQElements to resources available
        """
        results = []
        self.reorderList()
        for element in self.elements:
            # Act of matching returns the remaining match attributes
            matched, conditions = element.match(conditions)
            if matched:
                results.append(element)
                if not conditions:
                    break # stop matching when no resources left      
        return results


    def reorderList(self):
        """
        Order internal queue in priority order, highest first.
        """
        self.elements.sort()    


    def setStatus(self, subscription, status):
        """
        _setStatus_
        """
        # set the status of given subscriptions and status
        results = []
        for element in self.elements:
            if element.subscription == subscription:
                element.setStatus(status)
                results.append(element)
        return results


    def setPriority(self, newpriority, *workflows):
        """
        Update priority for an element and re-prioritize the queue
         Return number of affected blocks or -1 for error
        """
        #TODO: Error handling?
        wflows = lambda x: x.specUrl in workflows
        affected = self.mark(wflows, 'priority', newpriority)
        if affected:
            self.reorderList()
        return affected > 0


    def updateLocationInfo(self):
        """
        Update locations for elements
        """
        for element in self.elements:
            element.updateLocations(self.dbsHelper)


    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        """
        results = []
        # update the location information in WorkQueue
        # if this is too much overhead use separate component
        self.updateLocationInfo()
        #subscriptions = []
        #for site in siteJobs.key():
        # might just return one  block
        wqElementList = self.match(siteJobs)
        for wqElement in wqElementList:
            wmSpec = WorkSpecParser(wqElement.specUrl)
            wmbsHelper = WMBSHelper(wmSpec)
            # create fileset workflow and subscription
            # generate workflow name from wmSpec names
            workflowName = "Workflow"

            # generate fileset name from multiple blocks using some convention.
            # fileset should be blocks processed in the same sites
            filesetName = "Fileset"
            try:
                subscription = wmbsHelper.createSubscription(filesetName=workflowName,
                                                	     workflowName=workflowName)
            except:
                subscription = 1 #Remove when this works!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            results.append(subscription)
            wqElement.subscription = subscription

        return results

    
    def gotWork(self, *subscriptions):
        """
        _gotWork_
        
        this is called by JSM
        update the WorkQueue status table and remove from further consideration
        """
        subs = lambda x: x.subscription in subscriptions
        return self.mark(subs, 'status', 'Acquired') > 0


    def doneWork(self, *subscriptions):
        """
        _doneWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        subs = lambda x: x.subscription in subscriptions
        return self.mark(subs, 'status', 'Done') > 0


    def mark(self, searcher, field, newvalue):
        """
        Iterate over queue, replacing oldvalue with newvalue
        """
        count = 0
        for ele in self.elements:
            if searcher(ele):
                setattr(ele, field, newvalue)
                count += 1
        return count


    def queueWork(self, wmspec):
        """
        Take and queue work from a WMSpec
        """
        spec = WorkSpecParser(wmspec)
        for unit in spec:
            name = unit.name
            primaryBlock = unit.primaryBlock
            blocks = unit.blocks
            jobs = unit.jobs
            
            self.addElement(specUrl = wmspec,
                            primaryBlock = primaryBlock,
                            blocks = blocks,
                            priority = spec.priority(),
                            whiteList = spec.siteWhitelist(),
                            blackList = spec.siteBlacklist(),
                            nJobs = jobs)
        return True
