#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing
  
WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

__revision__ = "$Id: WorkQueue.py,v 1.10 2009/06/19 14:19:40 swakef Exp $"
__version__ = "$Revision: 1.10 $"

import time
# pylint: disable-msg=W0104,W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg=W0104,W0622
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from WMCore.DataStructs.WMObject import WMObject
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser

#TODO: Need to update spec file on some changes.
#TODO: What to persist. Most quantities can be loaded out of the spec file
# so dont have to be persisted. Though some quantities (priority) need
# to be stored per block, blocks within a request may have different priorities
#TODO: Load queue on restart, reparse all specs.
#TODO: Scale test
#TODO: Re-examine the way status changes (mark function) are made etc..

class _WQElement(WorkQueueBase):
    """
    _WQElement_

    WQElement container

    """
    def __init__(self, specHelper, njobs, primaryBlock = None, blocks = None):
        WorkQueueBase.__init__(self)
        self.wmSpec = specHelper
        self.primaryBlock = primaryBlock
        blocks = blocks or []
        # blockLocations is dict format of {blockName:[location List], ...}
        self.blockLocations = {}
        if primaryBlock:
            self.blockLocations[primaryBlock] = []
        for block in blocks:
            self.blockLocations[block] = []
        self.priority = self.wmSpec.priority
        self.nJobs = njobs
        self.insert_time = time.time()
        self.status = "Available"
        self.subscription = None


    def create(self):
        """
        Insert the work queue element to DB
        """
        existingTransaction = self.beginTransaction()
        self._insertWMSpec()
        self._insertBlock()
        self._insertWorkQueueElement()
        self.commitTransaction(existingTransaction)

    def _insertWMSpec(self):
        """
        """
        wmSpecAction = self.daofactory(classname = "WMSpec.New")
        #TODO: need a unique value (name?) for first parameter
        wmSpecAction.execute(self.wmSpec.specUrl, conn = self.getDBConn(),
                             transaction = self.existingTransaction())
                
    def _insertBlock(self):
        """
        """
        def _blockCreation(self, block):
            """
            """
            blockAction.execute(block["Name"], block["Size"], block["NumEvent"],
                                block["NumFiles"], conn = self.getDBConn(),
                                transaction = self.existingTransaction())
                
            for site in block["Locations"]:
                siteAction.execute(site, conn = self.getDBConn(),
                                   transaction = self.existingTransaction())
                
            self._updatedBlockLocations(block["Name"], block["Locations"])
        
        blockAction = self.daofactory(classname = "Block.New")
        siteAction = self.daofactory(classname = "Site.New")
        blockParantageAction = self.daofactory(classname = "Block.Parentage")
            
        _blockCreation(self.primaryBlock)
        for block in self.parentBlocks:
            _blockCreation(block)
            blockParantageAction.execute(self.primaryBlock["Name"], 
                                         block["Name"],
                                         conn = self.getDBConn(),
                                         transaction = self.existingTransaction())
    
    def _insertWorkQueueElement(self):
        """
        """
        wqAction = self.daofactory(classname = "WorkQueueElement.New")
        if self.parentBlocks:
            parentFlag = 1
        else:
            parentFlag = 0
        wqAction.execute(self.wmSpec.specUrl, self.primaryBlock["Name"], 
                         self.nJobs, self.priority, parentFlag,
                         conn = self.getDBConn(),
                         transaction = self.existingTransaction())
        
    def load(self, id, wmspecID, blockID, numJobs, priority, parentFlag):
        """
        Load work queue element by various tables from DB
        """
        wmSpecLoad = self.daofactory(classname = "WMSpec.LoadByID")
        
        self.specUrl = wmSpecLoad.execute(blockID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
        wqBlockLoad = self.daofactory(classname = "Block.LoadByID")
        
        self.primaryBlock = wqBlockLoad.execute(blockID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
        wqSiteLoad = self.daofactory(classname = "Site.LoadByBlockID")
        locations =  wqSiteLoad.execute(blockID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
        self.primaryBlock["Locations"] = locations 
        
        if parentFlag:
            wqParentAction = self.daofactory(classname = "Block.GetParentsByChildID")
            self.parentBlocks = wqParentAction.execute(blockID, conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
            # update information
            for parentBlock in self.parentBlocks:
                locations =  wqSiteLoad.execute(parentBlock["id"], conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        
                parentBlock["Locations"] = locations 
           
        self.priority = priority
        #TODO: need to get info somewhere
        self.nJobs = numJobs
        #self.lateUpdated = None
        self.status = "Available"
        self.subscription = None


    def __cmp__(self, other):
        """
        Compare work units so they can be sorted by priority
          priority determined by a weighted average of the 
            static priority and the queueing time
        """
        current = time.time()
        weight = 0.01
        lhs = self.priority + weight * (current - self.insert_time)
        rhs = other.priority + weight * (current - other.insert_time)
        return cmp(rhs, lhs) # Contrary to normal sorting (high priority first)


    def __str__(self):
        return "WQElement: %s:%s priority=%s, nJobs=%s, time=%s" % (self.wmSpec.specUrl,
                                    self.primaryBlock or "All", self.priority,
                                    self.nJobs, self.insert_time)


    def online(self):
        """
        Are all blocks online?
        """
        #TODO: Need to track this per block & site - for now fake it
        return True
    online = property(online)


    def locations(self):
        """
        _locations_
        
        return list of locations in the WQElement only if there is some common location
        in a primary block and its parent block
        """
        commonLocation = set(self.blockLocations[self.primaryBlock]) - \
                                                    set(self.wmSpec.blacklist)
        for locations in self.blockLocations.values():
            commonLocation = commonLocation.intersection(set(locations))
        if self.wmSpec.whitelist: # if given only return sites in the whitelist
            commonLocation = commonLocation & set(self.wmSpec.whitelist)
        return list(commonLocation)
    locations = property(locations)


    def dataAvailable(self, site):
        """
        Can we run at the given site?
        """
        if not self.blockLocations:
            return True # no input blocks - match any site
        #TODO: Stager behavior faked for now - should be site specific
        return self.online and site in self.locations
    
    
    def slotsAvailable(self, slots):
        """
        Do we have enough slots available?
        """
        #Options: Strict matching?, x% of block slots available?
        #For now strict - assume block small enough to release
        return slots >= self.nJobs


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
                newslots = slots - self.nJobs
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
        

    def updateLocations(self, dbsHelper):
        """
        _updateLocations_
        
        update all the block locations in the WQElement 
        """
        for block in self.blockLocations.keys():
            locations = dbsHelper.listFileBlockLocation(block)
            # just replace the location since the previous location might be
            # deleted 
            self.blockLocations[block] = locations

# dont track this in db anymore
#        for block in self.parentBlocks:
#            locations = dbsHelper.getBlockLocations(block["Name"])
#            # just replace the location since the previous location might be
#            # deleted 
#            block["Locations"] = locations    
#
#    def _updatedBlockLocations(self, block):
#        blockSiteDetele = self.daofactory(classname = "Block.DeleteSiteAssoc")
#        blockSiteAdd = self.daofactory(classname = "Block.AddSiteAssoc")
#        blockSiteDetele.execute(block["Name"], conn = self.getDBConn(),
#                                transaction = self.existingTransaction())
#        for site in block["Locations"]:
#            blockSiteAdd.execute(block["Name"], site, conn = self.getDBConn(),
#                                   transaction = self.existingTransaction())
        

class WorkQueue:
    """
    _WorkQueue_
    
    collection of work queue elements, 
    
    This  provide API for JSM (WorkQueuePool) - getWork(), gotWork()
    and injector
    """
    def __init__(self):
        self.elements = [ ]
        self.dbsHelpers = {}


    def __len__(self):
        return len(self.elements)


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
         Return True if status of any blocks changed else False
        """
        #TODO: Error handling?
        wflows = lambda x: x.wmSpec.specUrl in workflows
        affected = self.mark(wflows, 'priority', newpriority)
        if affected:
            self.reorderList()
        return affected > 0


    def updateLocationInfo(self):
        """
        Update locations for elements
        """
        for element in self.elements:
            dbs = self.dbsHelpers[element.wmSpec.dbs_url]
            element.updateLocations(dbs)


    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        """
        results = []
        # update the location information in WorkQueue
        # if this is too much overhead use separate component
        self.updateLocationInfo()
        wqElementList = self.match(siteJobs)
        for wqElement in wqElementList:
            wmbsHelper = WMBSHelper(wqElement.wmSpec.wmSpec)
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
        #TODO: Error handling?
        subs = lambda x: x.subscription in subscriptions
        return self.mark(subs, 'status', 'Acquired') > 0


    def doneWork(self, *subscriptions):
        """
        _doneWork_
        
        this is called by JSM
        update the WorkQueue status form table
        """
        #TODO: Error handling?
        subs = lambda x: x.subscription in subscriptions
        return self.mark(subs, 'status', 'Done') > 0


    def mark(self, searcher, field, newvalue):
        """
        Iterate over queue, setting field to newvalue
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
        
        if not self.dbsHelpers.has_key(spec.dbs_url):
            self.dbsHelpers[spec.dbs_url] = DBSReader(spec.dbs_url)
        
        for unit in spec.split(dbs_pool = self.dbsHelpers):
            primaryBlock = unit.primaryBlock
            blocks = unit.blocks
            jobs = unit.jobs
            
            ele = _WQElement(spec, jobs, primaryBlock, blocks)
            #TODO: Commit element to database
            self.elements.append(ele)
        return True
