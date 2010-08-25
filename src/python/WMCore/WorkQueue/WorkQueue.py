#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing
  
WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool 
"""

__revision__ = "$Id: WorkQueue.py,v 1.12 2009/06/24 21:00:23 sryu Exp $"
__version__ = "$Revision: 1.12 $"

import time
# pylint: disable-msg=W0104,W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg=W0104,W0622
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

from WMCore.DataStructs.WMObject import WMObject
from WMCore.WorkQueue.DBSHelper import DBSHelper
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser
from WMCore.WorkQueue.DataStructs.Block import Block

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
    def __init__(self, specHelper, nJobs, insertTime = None, primaryBlock = None, blocks = None):
                 
        WorkQueueBase.__init__(self)
        self.wmSpec = specHelper
        self.primaryBlock = primaryBlock
        self.parentBlocks = blocks or []
        self.priority = self.wmSpec.priority
        self.nJobs = nJobs
        #TODO: need to set it NONE
        self.insertTime = insertTime 
        self.status = "Available"
        self.subscription = None
            
    def create(self):
        """
        Insert the work queue element to DB
        """
        existingTransaction = self.beginTransaction()
        self._insertWMSpec()
        if self.primaryBlock: 
            self._insertBlock()
        self._insertWorkQueueElement()
        self.commitTransaction(existingTransaction)

    def _insertWMSpec(self):
        """
        """
        existsAction = self.daofactory(classname = "WMSpec.Exists")
        exists = existsAction.execute(self.wmSpec.name, conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
        if not exists:
            wmSpecAction = self.daofactory(classname = "WMSpec.New")
            #TODO: need a unique value (name?) for first parameter
            wmSpecAction.execute(self.wmSpec.name, self.wmSpec.specUrl, 
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction())
                
    def _insertBlock(self):
        """
        """
        def _blockCreation(blockInfo):
            """
            """
            blockAction.execute(blockInfo["Name"], blockInfo["Size"], blockInfo["NumEvents"],
                                blockInfo["NumFiles"], conn = self.getDBConn(),
                                transaction = self.existingTransaction())
                
            
        blockAction = self.daofactory(classname = "Block.New")
        blockParantageAction = self.daofactory(classname = "Block.AddParent")
            
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
        if self.primaryBlock:
            blockName = self.primaryBlock["Name"]
        else:
            blockName = "NoBlock"
        if self.parentBlocks:
            parentFlag = 1
        else:
            parentFlag = 0
        #TODO: need to handle properly production job: check whether unique contraint is correct on wmspec table 
        existsAction = self.daofactory(classname = "WorkQueueElement.Exists")
        exists = existsAction.execute(self.wmSpec.name, blockName, conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
        if not exists:
            wqAction.execute(self.wmSpec.name, blockName, 
                             self.nJobs, self.priority, parentFlag,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        
    
    def __cmp__(self, other):
        """
        Compare work units so they can be sorted by priority
          priority determined by a weighted average of the 
            static priority and the queueing time
        """
        current = time.time()
        weight = 0.01
        lhs = self.priority + weight * (current - self.insertTime)
        rhs = other.priority + weight * (current - other.insertTime)
        return cmp(rhs, lhs) # Contrary to normal sorting (high priority first)


    def __str__(self):
         return "WQElement: %s:%s priority=%s, nJobs=%s, time=%s" % (self.wmSpec.specUrl,
                                    self.primaryBlock or "All", self.priority,
                                    self.nJobs, self.insertTime)



    def online(self):
        """
        Are all blocks online?
        """
        return True
    online = property(online)
	   
    def locations(self):
        """
        _locations_
        
        return list of locations in the WQElement only if there is some common location
        in a primary block and its parent block
        """
        commonLocation = set(self.primaryBlock["Locations"])
        for block in self.parentBlocks:
            commonLocation = commonLocation.intersection(
                                        set(block["Locations"]))
        
        if self.wmSpec.whitelist: # if given only return sites in the whitelist
            commonLocation = commonLocation & set(self.wmSpec.whitelist)
        return list(commonLocation)
    locations = property(locations)
    
    def dataAvailable(self, site):
        """
        Can we run at the given site?
        """
        
        if not self.primaryBlock:
            return True # no input blocks - match any site
        
        commonLocations = self.locations
        #TODO: Stager behavior faked for now - should be site specific
        return self.online and site in commonLocations
    
    
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
        #print "%%%%%%%%%%%%%%%%%%%%"
        #print self.primaryBlock
        #if not self.primaryBlock:
        #    print "primary block is none"
        #    return
        locations = dbsHelper.listFileBlockLocation(self.primaryBlock["Name"])
            # just replace the location since the previous location might be
            # deleted 
        self.primaryBlock["Locations"] = locations    

        for block in self.parentBlocks:
            locations = dbsHelper.getBlockLocations(block["Name"])
            # just replace the location since the previous location might be
            # deleted 
            block["Locations"] = locations    


class WorkQueue(WorkQueueBase):
    """
    _WorkQueue_
    
    collection of work queue elements, 
    
    This  provide API for JSM (WorkQueuePool) - getWork(), gotWork()
    and injector
    """
    def __init__(self):
        WorkQueueBase.__init__(self)
        self.wmSpecs = {}
        self.elements = {} 
        self.dbsHelpers = {}


    def __len__(self):
        return len(self.elements.values())

    def load(self):
        """
        load all available work queue element from DB by priorty order.
        TODO: the selection can be fine grained if it takes up to much memory space
        """
        wqAction = self.daofactory(classname = "WorkQueueElement.GetElements")
        elements = wqAction.execute(conn = self.getDBConn(),
                                         transaction = self.existingTransaction())
        for ele in elements:
            if ele["id"] not in self.elements.keys():
            #just update the priority
                wqEle = self._getWQElement(ele["wmspec_id"], ele["block_id"], 
                       ele["num_jobs"], ele["insert_time"])
                self.elements[ele["id"]] = wqEle
        self.updateLocationInfo()
    
    def _getWQElement(self, wmSpecID, blockID, nJobs, insertTime):
        """
        get WQElement using DAO
        TODO this would make more sense to be in _WQElement class. need to put it 
        there if there is efficient way 
        Handle production job currently block id will be 1 for that with block name "NoBlocks"
        Need to handle properly
        """

        wmSpecLoad = self.daofactory(classname = "WMSpec.LoadByID")
        
        specInfo = wmSpecLoad.execute(wmSpecID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        if specInfo[0]["name"] in self.wmSpecs.keys():
            wmSpec = self.wmSpecs[specInfo[0]["name"]]
        else:
            wmSpec = WorkSpecParser(specInfo[0]["url"])
            self.wmSpecs[specInfo[0]["name"]] = wmSpec

        wqBlockLoad = self.daofactory(classname = "Block.LoadByID")
        
        blockInfo = wqBlockLoad.execute(blockID, conn = self.getDBConn(),
                                        transaction = self.existingTransaction())
        primaryBlock = Block.getBlock(blockInfo)
        parentBlocks = None 

        if wmSpec.parentFlag:
            wqParentAction = self.daofactory(classname = "Block.GetParentsByChildID")
            blocks = wqParentAction.execute(blockID, conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
            parentBlocks = []
            for block in blocks:
                parentBlocks.append(Block.getBlock(block))
                
        return _WQElement(wmSpec, nJobs, insertTime, primaryBlock, parentBlocks)
    
    def match(self, conditions):
        """
        Loop over internal queue in priority order, 
          matching WQElements to resources available
        """
        results = []
        self.reorderList()
        for element in self.elements.values():
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
        self.elements.values().sort()    


    def setStatus(self, subscription, status):
        """
        _setStatus_
        """
        # set the status of given subscriptions and status
        results = []
        for element in self.elements.values():
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
        #print "@@@@@@@@@@"
        #print self.elements
        for element in self.elements.values():
            #print "----------------------"           
            #print element.primaryBlock
            dbs = self.dbsHelpers[element.wmSpec.dbs_url]
            element.updateLocations(dbs)


    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        """
        # always populate the self.elements freshly before the selection 
        self.load()
        
        results = []
        subscriptions = []
        #for site in siteJobs.key():
        # might just return one  block
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
            
            #print "----- %s" % primaryBlock
            ele = _WQElement(spec, jobs, primaryBlock, blocks)
            ele.create()
            # only update in database 
            #self.elements.append(ele)
        return True
