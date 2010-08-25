#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing

WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

__revision__ = "$Id: WorkQueue.py,v 1.24 2009/08/24 16:33:14 sryu Exp $"
__version__ = "$Revision: 1.24 $"

# pylint: disable-msg = W0104, W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg = W0104, W0622

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser
from WMCore.WMBS.Subscription import Subscription as WMBSSubscription
from WMCore.WMBS.File import File as WMBSFile



#TODO: Black/White list not taken into account
#TODO: Scale test
#TODO: Handle multiple dbs instances


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
        self.itemWeight = 0.01  # weight for queuing time weighted average


    def __len__(self):
        items = self.daofactory(classname = "WorkQueueElement.GetElements")
        return len(items.execute(conn = self.getDBConn(),
                             transaction = self.existingTransaction()))


    def _insertWorkQueueElement(self, wmspec, nJobs, primaryBlock,
                                parentBlocks, subscription):
        """
        Persist a block to the database
        """
        self._insertWMSpec(wmspec)
        #still need to insert fack block for production job
        #if primaryBlock['NumFiles'] != 0: #TODO: change this
        self._insertBlock(primaryBlock, parentBlocks)

        wqAction = self.daofactory(classname = "WorkQueueElement.New")
        parentFlag = parentBlocks and 1 or 0
        wqAction.execute(wmspec.name, primaryBlock['Name'], nJobs,
                             wmspec.priority, parentFlag, subscription,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())


    def _insertWMSpec(self, wmSpec):
        """
        """
        existsAction = self.daofactory(classname = "WMSpec.Exists")
        exists = existsAction.execute(wmSpec.name, conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        if not exists:
            wmSpecAction = self.daofactory(classname = "WMSpec.New")
            #TODO: need a unique value (name?) for first parameter
            wmSpecAction.execute(wmSpec.name, wmSpec.specUrl,
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction())

    def _insertBlock(self, primaryBlock, parentBlocks):
        """
        Insert blocks and record parentage info
        """
        def _blockCreation(blockInfo):
            """
            Internal function to insert a block
            """
            blockAction.execute(blockInfo["Name"], blockInfo["Size"],
                                blockInfo["NumEvents"], blockInfo["NumFiles"],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        blockAction = self.daofactory(classname = "Block.New")
        blockParentageAct = self.daofactory(classname = "Block.AddParent")

        _blockCreation(primaryBlock)
        for block in parentBlocks:
            _blockCreation(block)
            blockParentageAct.execute(primaryBlock["Name"],
                                      block["Name"],
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

    def _insertSite(self, *sites):
        """
        Insert site into database
        """
        siteAction = self.daofactory(classname = "Site.New")
        siteAction.execute(sites,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())


    def match(self, conditions):
        """
        Match resources to available work
        """
        matchAction = self.daofactory(classname = "WorkQueueElement.GetWork")
        elements = matchAction.execute(conditions, self.itemWeight,
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())
        return elements


    def setStatus(self, status, *subscriptions):
        """
        _setStatus_, throws an exception if no elements are updated
        """
        subscriptions = [str(x['id']) for x in subscriptions]
        updateAction = self.daofactory(classname =
                                       "WorkQueueElement.UpdateStatus")
        affected = updateAction.execute(status, subscriptions,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        if not affected:
            raise RuntimeError, "Status not changed: No matching elements"


    def setPriority(self, newpriority, *workflowNames):
        """
        Update priority for a workflow, throw exception if no elements affected
        """
        updateAction = self.daofactory(classname = "WorkQueueElement.UpdatePriority")
        affected = updateAction.execute(newpriority, workflowNames,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        if not affected:
            raise RuntimeError, "Priority not changed: No matching elements"


    def updateLocationInfo(self):
        """
        Update locations for elements
        """
        #self.beginTransaction()
        #get blocks and dbsurls (for no assume global!)
        blocksAction = self.daofactory(classname = "Block.GetActiveBlocks")
        mappingAct = self.daofactory(classname = "Site.UpdateBlockSiteMapping")
        blocks = blocksAction.execute(conn = self.getDBConn(),
                                      transaction = self.existingTransaction())
        result = {}
        dbs = self.dbsHelpers.values()[0] #FIXME!!!
        uniqueLocations = set()
        for block in blocks:
            locations = dbs.listFileBlockLocation(block['name'])
            result[block['name']] = locations
            for location in locations:
                uniqueLocations.add(location)

        siteAction = self.daofactory(classname = "Site.New")
        siteAction.execute(tuple(uniqueLocations), conn = self.getDBConn(),
                           transaction = self.existingTransaction())

        # map blocks to locations (in one call?)
        mappingAct.execute(result, conn = self.getDBConn(),
                           transaction = self.existingTransaction())
        #self.commitTransaction(self.existingTransaction())

    def getWork(self, siteJobs):
        """
        _getWork_
        siteJob is dict format of {site: estimateJobSlot}
        
        JobCreator calls this method, it will 
        1. match jobs with work queue element
        2. create the subscription for it if it is not already exist. 
           (currently set to have one subscription per a workload)
           (associate the subscription to workload - currently following naming convention,
            so it can retrieved by workflow name - but might needs association table)
        3. fill up the fileset with files in the subscription 
           when if it is processing jobs. if it is production jobs (MC) fileset will be empty
        4. TODO: close the fileset if the last workqueue element of the workload is processed. 
        5. update the workqueue status to ('Acquired') might need finer status change 
           if it fails to create wmbs files partially
        6. return list of subscription (or not)
           it can be only tracked only subscription (workload) level job done
           or
           return workquue element list:
           if we want to track partial level of success. But requires JobCreate map workqueue element
           to each jobgroup. also doneWork parameter should be list of workqueue element not list 
           of subscription
        """
        results = []
        blockLoader = self.daofactory(classname = "Block.LoadByID")
        parentBlockLoader = \
                    self.daofactory(classname = "Block.GetParentsByChildID")
        matches = self.match(siteJobs)
        for match in matches:
            #wmbsHelper = WMBSHelper(wqElement.wmSpec)
            #TODO: task maker will handle creating the subscription
            #It will be already available by now - wqElement.wmSpec.subscriptionID?
            #subscription = wmbsHelper.createSubscription()
            #TODO: also fill up the files in the fileset
            #      find out how to handle parent files
            #dbs = self.dbsHelpers[wqElement.wmSpec.dbs_url]
            #files, pfile = wqElement.listFilesInElement(dbs)
            #wmbsHelper.createFilesAndAssociateToFileset(files)

            #TODO: probably need to pass element id list as well if it needs track
            # fine grained status
            # also check if it is the last element in the given spec close the fileset.
            #results.append(subscription)
            #wqElement.subscription = subscription
            #TODO: probably need to update the status here since this is not REST call.
            # gotWork function won't be necessary

            sub = WMBSSubscription(id = match['subscription_id'])
            sub.load()

            dbs = self.dbsHelpers.values()[0] #FIXME!!!
            if match['block_id']:
                block = blockLoader.execute(match['block_id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
                if match['parent_flag']:
                    dbsBlock = dbs.getFileBlockWithParents(block["name"])[block['name']]
                else:
                    dbsBlock = dbs.getFileBlock(block["name"])[block['name']]

                # should this be moved into gotWork()
                fileset = sub["fileset"]
                for dbsFile in dbsBlock['Files']:
                    wmbsFile = WMBSFile(lfn = dbsFile["LogicalFileName"],
                            size = dbsFile["FileSize"],
                            events = dbsFile["NumberOfEvents"],
                            cksum = dbsFile["Checksum"],
                            parents = dbsFile["ParentList"],
                            locations = set(dbsBlock['StorageElements']))
                    fileset.addFile(wmbsFile)
                fileset.commit()
            results.append(sub)
        return results


    def gotWork(self, *subscriptions):
        """
        _gotWork_

        this is called by JSM
        update the WorkQueue status table and remove from further consideration
        """
        self.setStatus('Acquired', *subscriptions)


    def doneWork(self, *subscriptions):
        """
        _doneWork_

        this is called by JSM
        update the WorkQueue status table
        """
        self.setStatus('Done', *subscriptions)


    def queueWork(self, wmspec):
        """
        Take and queue work from a WMSpec
        """
        spec = WorkSpecParser(wmspec)

        if not self.dbsHelpers.has_key(spec.dbs_url):
            self.dbsHelpers[spec.dbs_url] = DBSReader(spec.dbs_url)

        units = spec.split(dbs_pool = self.dbsHelpers)

        #TODO: Look at db transactions - try to minimize time active
        self.beginTransaction()

        for primaryBlock, blocks, jobs in units:
            wmbsHelper = WMBSHelper(spec, primaryBlock['Name'])
            sub = wmbsHelper.createSubscription()

            self._insertWorkQueueElement(spec, jobs, primaryBlock,
                                         blocks, sub['id'])

        self.commitTransaction(self.existingTransaction())
