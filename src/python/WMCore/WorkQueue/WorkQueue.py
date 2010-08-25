#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing

WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

__revision__ = "$Id: WorkQueue.py,v 1.30 2009/09/24 20:17:42 sryu Exp $"
__version__ = "$Revision: 1.30 $"

# pylint: disable-msg = W0104, W0622
try:
    set
except NameError:
    from sets import Set as set
# pylint: enable-msg = W0104, W0622

from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WMBSHelper import WMBSHelper
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkQueue.WorkSpecParser import WorkSpecParser
from WMCore.WMBS.Subscription import Subscription as WMBSSubscription
from WMCore.WMBS.File import File as WMBSFile



#TODO: Black/White list not taken into account
#TODO: Scale test
#TODO: Handle multiple dbs instances
#TODO: Warning dataset level global queues don't take account of location

class WorkQueue(WorkQueueBase):
    """
    _WorkQueue_

    collection of work queue elements,

    This  provide API for JSM (WorkQueuePool) - getWork(), gotWork()
    and injector
    """
    def __init__(self, **params):
        WorkQueueBase.__init__(self)
        self.wmSpecs = {}
        self.elements = {}
        self.dbsHelpers = {}
        self.params = params
        self.params.setdefault('ParentQueue', None) # Get more work from here
        self.params.setdefault('QueueDepth', 100) # when less than this locally
        self.params.setdefault('SplitByBlock', True)
        self.params.setdefault('ItemWeight', 0.01) # Queuing time weighted avg


    def __len__(self):
        items = self.daofactory(classname = "WorkQueueElement.GetElements")
        return len(items.execute(conn = self.getDBConn(),
                             transaction = self.existingTransaction()))


    def _insertWorkQueueElement(self, wmspec, nJobs, primaryInput,
                                parentInputs, subscription, parentQueueId):
        """
        Persist a block to the database
        """
        self._insertWMSpec(wmspec)
        if primaryInput:
            self._insertInputs(primaryInput, parentInputs)

        wqAction = self.daofactory(classname = "WorkQueueElement.New")
        parentFlag = parentInputs and 1 or 0
        wqAction.execute(wmspec.name, primaryInput, nJobs,
                             wmspec.priority, parentFlag, subscription,
                             parentQueueId, conn = self.getDBConn(),
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

    def _insertInputs(self, primary, parents):
        """
        Insert blocks and record parentage info
        """
        def _inputCreation(data):
            """
            Internal function to insert an input
            """
            dataAction.execute(data, conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        dataAction = self.daofactory(classname = "Data.New")
        dataParentageAct = self.daofactory(classname = "Data.AddParent")

        _inputCreation(primary)
        for parent in parents:
            _inputCreation(parent)
            dataParentageAct.execute(primary,
                                      parent,
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
        elements = matchAction.execute(conditions, self.params['ItemWeight'],
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())
        return elements


    def setStatus(self, status, *subscriptions):
        """
        _setStatus_, throws an exception if no elements are updated
        """
        #subscriptions = [str(x['id']) for x in subscriptions]
        updateAction = self.daofactory(classname =
                                       "WorkQueueElement.UpdateStatus")
        affected = updateAction.execute(status, subscriptions,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        if not affected:
            raise RuntimeError, "Status not changed: No matching elements"

        if self.params['ParentQueue'] and status in ('Done', 'Failed'):
            # get parent id's
            parentAction = self.daofactory(classname =
                                       "WorkQueueElement.GetParentId")
            affected = parentAction.execute(subscriptions,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
            self.params['ParentQueue'].setStatus(status, *([x['parent_queue_id'] for x in affected]))


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
        blocksAction = self.daofactory(classname = "Data.GetActiveData")
        mappingAct = self.daofactory(classname = "Site.UpdateDataSiteMapping")
        blocks = blocksAction.execute(conn = self.getDBConn(),
                                      transaction = self.existingTransaction())
        if not blocks:
            return
        result = {}
        dbs = self.dbsHelpers.values()[0] #FIXME!!!
        uniqueLocations = set()
        for block in blocks:
            locations = dbs.listFileBlockLocation(block['name'])
            result[block['name']] = locations
            for location in locations:
                uniqueLocations.add(location)

        if uniqueLocations:
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

        # Probably should move somewhere that doesn't block the client
        self.pullWork(siteJobs)
        self.updateLocationInfo()

        results = []
        blockLoader = self.daofactory(classname = "Data.LoadByID")
        parentBlockLoader = \
                    self.daofactory(classname = "Data.GetParentsByChildID")
        matches = self.match(siteJobs)
        for match in matches:

            sub = WMBSSubscription(id = match['subscription_id'])
            sub.load()
            sub['workflow'].load()

            if match['input_id']:
                dbs = self.dbsHelpers.values()[0] #FIXME!!!
                block = blockLoader.execute(match['input_id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
                
                if match['parent_flag']:
                    dbsBlock = dbs.getFileBlockWithParents(block["name"])[block['name']]
                else:
                    dbsBlock = dbs.getFileBlock(block["name"])[block['name']]

                # TODO: parent fileset
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
        if results:
            self.setStatus('Acquired', *([str(x['id']) for x in results]))
        return results


#    def gotWork(self, *subscriptions):
#        """
#        _gotWork_
#
#        this is called by JSM
#        update the WorkQueue status table and remove from further consideration
#        """
#        self.setStatus('Acquired', *subscriptions)


    def doneWork(self, *subscriptions):
        """
        _doneWork_

        this is called by JSM
        update the WorkQueue status table
        """
        self.setStatus('Done', *subscriptions)


    def queueWork(self, wmspecUrl, parentQueueId = None):
        """
        Take and queue work from a WMSpec
        """
        wmspec = WMWorkloadHelper()
        wmspec.load(wmspecUrl)
        
        for topLevelTask in wmspec.taskIterator():
            specPaser = WorkSpecParser(topLevelTask)
            dbsUrl = topLevelTask.dbsUrl()
            if dbsUrl and not self.dbsHelpers.has_key(dbsUrl):
                self.dbsHelpers[dbsUrl] = DBSReader(dbsUrl)
    
            units = specPaser.split(split = self.params['SplitByBlock'],
                                    dbs_pool = self.dbsHelpers)
    
            #TODO: Look at db transactions - try to minimize time active
            self.beginTransaction()
    
            for primaryBlock, blocks, jobs in units:
                wmbsHelper = WMBSHelper(topLevelTask, primaryBlock)
                sub = wmbsHelper.createSubscription()
    
                self._insertWorkQueueElement(wmspec, jobs, primaryBlock,
                                             blocks, sub['id'], parentQueueId)
    
            self.commitTransaction(self.existingTransaction())


    def pullWork(self, resources):
        """
        Pull work from another WorkQueue to be processed
        """
        # Should this use job count instead
        # monitor queue depth (jobs) per site?
        if not self.params['ParentQueue'] or \
                                        len(self) > self.params['QueueDepth']:
            return

        work = self.params['ParentQueue'].getWork(resources)
        for element in work:
            self.queueWork(element['workflow'].spec,
                           parentQueueId = element['id'])
        self.params['ParentQueue'].setStatus('Acquired', *([x['id'] for x in work]))
