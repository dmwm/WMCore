#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing

WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

import types
from collections import defaultdict
import os
import threading

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB

from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
from WMCore.WorkQueue.Policy.Start import startPolicy
from WMCore.WorkQueue.Policy.End import endPolicy
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueUtils import get_dbs

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, getWorkloadFromTask
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.WorkQueue.DataLocationMapper import WorkQueueDataLocationMapper

from WMCore.Database.CMSCouch import CouchNotFoundError



#  //
# // Convenience constructor functions
#//
def globalQueue(logger = None, dbi = None, **kwargs):
    """Convenience method to create a WorkQueue suitable for use globally
    """
    defaults = {'PopulateFilesets' : False,
                'LocalQueueFlag': False,
                'SplittingMapping' : {'DatasetBlock' : 
                                        {'name': 'Dataset', 
                                         'args': {}}
                                      },
                }
    defaults.update(kwargs)
    return WorkQueue(logger, dbi, **defaults)

def localQueue(logger = None, dbi = None, **kwargs):
    """Convenience method to create a WorkQueue suitable for use locally
    """
    defaults = {'TrackLocationOrSubscription' : 'location',
                'ParentQueueCouchUrl' : 'http://localhost:5984/workqueue_t_global'}
    defaults.update(kwargs)
    return WorkQueue(logger, dbi, **defaults)



class WorkQueue(WorkQueueBase):
    """
    _WorkQueue_

    WorkQueue object - interface to WorkQueue functionality.
    """
    def __init__(self, logger = None, dbi = None, **params):

        WorkQueueBase.__init__(self, logger, dbi)
        self.parent_queue = None
        self.params = params

        self.params.setdefault('CouchUrl', os.environ.get('COUCHURL'))
        if not self.params.get('CouchUrl'):
            raise RuntimeError, 'CouchUrl config value mandatory'
        self.params.setdefault('DbName', 'workqueue')
        self.params.setdefault('InboxDbName', self.params['DbName'] + '_inbox')
        self.params.setdefault('ParentQueueCouchUrl', None) # We get work from here

        self.backend = WorkQueueBackend(self.params['CouchUrl'], self.params['DbName'],
                                        self.params['InboxDbName'],
                                        self.params['ParentQueueCouchUrl'], self.params.get('QueueURL'),
                                        logger = self.logger)
        if self.params.get('ParentQueueCouchUrl'):
            self.parent_queue = WorkQueueBackend(self.params['ParentQueueCouchUrl'].rsplit('/', 1)[0],
                                                 self.params['ParentQueueCouchUrl'].rsplit('/', 1)[1])

        self.params.setdefault("GlobalDBS",
                               "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.params.setdefault('QueueDepth', 2) # when less than this locally
        self.params.setdefault('LocationRefreshInterval', 600)
        self.params.setdefault('FullLocationRefreshInterval', 7200)
        self.params.setdefault('TrackLocationOrSubscription', 'subscription')
        self.params.setdefault('ReleaseIncompleteBlocks', False)
        self.params.setdefault('ReleaseRequireSubscribed', True)
        self.params.setdefault('PhEDExEndpoint', None)
        self.params.setdefault('PopulateFilesets', True)
        self.params.setdefault('LocalQueueFlag', True)

        self.params.setdefault('JobDumpConfig', None)
        self.params.setdefault('BossAirConfig', None)

        self.params['QueueURL'] = self.backend.queueUrl # url this queue is visible on
                                    # backend took previous QueueURL and sanitized it
        self.params.setdefault('WMBSURL', None) # this will be only set on local Queue
        self.params.setdefault('Teams', [''])
        self.params.setdefault('DrainMode', False)
        if self.params.get('CacheDir'):
            try:
                os.makedirs(self.params['CacheDir'])
            except OSError:
                pass
        elif self.params.get('PopulateFilesets'):
            raise RuntimeError, 'CacheDir mandatory for local queue'

        self.params.setdefault('SplittingMapping', {})
        self.params['SplittingMapping'].setdefault('DatasetBlock',
                                                   {'name': 'Block',
                                                    'args': {}}
                                                  )
        self.params['SplittingMapping'].setdefault('MonteCarlo',
                                                   {'name': 'MonteCarlo',
                                                    'args':{}}
                                                   )
        self.params['SplittingMapping'].setdefault('Dataset',
                                                   {'name': 'Dataset',
                                                    'args': {}}
                                                  )
        self.params['SplittingMapping'].setdefault('Block',
                                                   {'name': 'Block',
                                                    'args': {}}
                                                  )
        self.params['SplittingMapping'].setdefault('ResubmitBlock',
                                                   {'name': 'ResubmitBlock',
                                                    'args': {}}
                                                  )
        
        self.params.setdefault('EndPolicySettings', {})

        assert(self.params['TrackLocationOrSubscription'] in ('subscription',
                                                              'location'))
        # Can only release blocks on location
        if self.params['TrackLocationOrSubscription'] == 'location':
            if self.params['SplittingMapping']['DatasetBlock']['name'] != 'Block':
                raise RuntimeError, 'Only blocks can be released on location'

        if self.params.get('PhEDEx'):
            self.phedexService = self.params['PhEDEx']
        else:
            phedexArgs = {}
            if self.params.get('PhEDExEndpoint'):
                phedexArgs['endpoint'] = self.params['PhEDExEndpoint']
            self.phedexService = PhEDEx(phedexArgs)

        if self.params.get('SiteDB'):
            self.SiteDB = self.params['SiteDB']
        else:
            self.SiteDB = SiteDB()

        if type(self.params['Teams']) in types.StringTypes:
            self.params['Teams'] = [x.strip() for x in \
                                    self.params['Teams'].split(',')]

        self.dataLocationMapper = WorkQueueDataLocationMapper(self.logger, self.backend,
                                                              phedex = self.phedexService,
                                                              sitedb = self.SiteDB,
                                                              locationFrom = self.params['TrackLocationOrSubscription'],
                                                              incompleteBlocks = self.params['ReleaseIncompleteBlocks'],
                                                              requireBlocksSubscribed = not self.params['ReleaseIncompleteBlocks'],
                                                              fullRefreshInterval = self.params['FullLocationRefreshInterval'],
                                                              updateIntervalCoarseness = self.params['LocationRefreshInterval'])

    def __len__(self):
        """Returns number of Available elements in queue"""
        return self.backend.queueLength()


    def setStatus(self, status, elementIDs = None, SubscriptionId = None, WorkflowName = None):
        """
        _setStatus_, throws an exception if no elements are updated

        """
        try:
            if not elementIDs:
                elementIDs = []
            iter(elementIDs)
            if type(elementIDs) in types.StringTypes:
                raise TypeError
        except TypeError:
            elementIDs = [elementIDs]

        if status == 'Canceled': # Cancel Needs special actions
            return self.cancelWork(elementIDs, SubscriptionId, WorkflowName)

        args = {}
        if SubscriptionId:
            args['SubscriptionId'] = SubscriptionId
        if WorkflowName:
            args['RequestName'] = WorkflowName

        affected = self.backend.getElements(elementIDs = elementIDs, **args)

        for x in affected:
            x['Status'] = status
        elements = self.backend.saveElements(*affected)
        
        if not affected:
            raise RuntimeError, "Status not changed: No matching elements"
        return elements

    def setPriority(self, newpriority, *workflowNames):
        """
        Update priority for a workflow, throw exception if no elements affected
        """
        affected = []
        for wf in workflowNames:
            affected.extend(self.backend.getElements(returnIdOnly = True, RequestName = wf))

        self.backend.updateElements(*affected, Priority = newpriority)

        if not affected:
            raise RuntimeError, "Priority not changed: No matching elements"

    def resetWork(self, ids):
        """Put work back in Available state, from here either another queue
         or wmbs can pick it up.

         If work was Acquired by a child queue, the next status update will
         cancel the work in the child.

         Note: That the same child queue is free to pick the work up again,
          there is no permanent blacklist of queues.
        """
        try:
            iter(ids)
        except TypeError:
            ids = [ids]

        return self.backend.updateElements(*ids, Status = 'Available',
                                           ChildQueueUrl = None, WMBSUrl = None)

    def getWork(self, siteJobs):
        """ 
        Get available work from the queue, inject into wmbs & mark as running

        siteJob is dict format of {site: estimateJobSlot}
        of the resources to get work for.
        """
        results = []
        if not self.backend.isAvailable():
            self.logger.info('Backend busy or down: skipping fetching of work')
            return results
        matches, _ = self.backend.availableWork(siteJobs)

        if not matches:
            return results

        # cache wmspecs for lifetime of function call, likely we will have multiple elements for same spec.
        #TODO: Check to see if we can skip spec loading - need to persist some more details to element
        wmspecCache = {}
        for match in matches:
            blockName, dbsBlock = None, None
            if self.params['PopulateFilesets']:
                if not wmspecCache.has_key(match['RequestName']):
                    wmspec = self.backend.getWMSpec(match['RequestName'])
                    wmspecCache[match['RequestName']] = wmspec
                else:
                    wmspec = wmspecCache[match['RequestName']]

                if match['Inputs']:
                    self.logger.info("Adding Processing work")
                    blockName, dbsBlock = self._getDBSBlock(match, wmspec)
                else:
                    self.logger.info("Adding Production work")

                match['Subscription'] = self._wmbsPreparation(match,
                                                              wmspec,
                                                              blockName,
                                                              dbsBlock)

            results.append(match)

        del wmspecCache # remove cache explicitly
        self.logger.info('Injected %s units into WMBS' % len(results))
        return results

    def _getDBSBlock(self, match, wmspec):
        """Get DBS info for this block"""
        blockName = match['Inputs'].keys()[0] #TODO: Allow more than one

        if match['ACDC']:
            acdcInfo = match['ACDC']
            acdc = DataCollectionService(acdcInfo["server"], acdcInfo["database"])
            collection = acdc.getDataCollection(acdcInfo['collection'])
            splitedBlockName = ACDCBlock.splitBlockName(blockName)
            fileLists = acdc.getChunkFiles(collection,
                                           acdcInfo['fileset'],
                                           splitedBlockName['Offset'],
                                           splitedBlockName['NumOfFiles'])
            return blockName, fileLists
        else:
            dbs = get_dbs(match['Dbs'])
            if wmspec.getTask(match['TaskName']).parentProcessingFlag():
                dbsBlockDict = dbs.getFileBlockWithParents(blockName)
            else:
                dbsBlockDict = dbs.getFileBlock(blockName)
        return blockName, dbsBlockDict[blockName]

    def _wmbsPreparation(self, match, wmspec, blockName, dbsBlock):
        """Inject data into wmbs and create subscription.
        """
        from WMCore.WorkQueue.WMBSHelper import WMBSHelper
        self.logger.info("Adding WMBS subscription")

        mask = match['Mask']
        wmbsHelper = WMBSHelper(wmspec, blockName, mask, self.params['CacheDir'])

        sub, match['NumOfFilesAdded'] = wmbsHelper.createSubscriptionAndAddFiles(block = dbsBlock)
        self.logger.info("Created top level Subscription %s with %s files" % (sub['id'], match['NumOfFilesAdded']))

        match['SubscriptionId'] = sub['id']
        match['Status'] = 'Running'
        self.backend.saveElements(match)

        self.logger.info('WMBS subscription (%s) created for wf: "%s"' 
                         % (sub['id'], match['RequestName']))
        return sub

    def _assignToChildQueue(self, queue, wmbsUrl,*elements):
        """Assign work to the provided child queue"""
        for ele in elements:
            ele['Status'] = 'Negotiating'
            ele['ChildQueueUrl'] = queue
            ele['WMBSUrl'] = wmbsUrl
        work = self.parent_queue.saveElements(*elements)
        requests = ', '.join(list(set(['"%s"' % x['RequestName'] for x in work])))
        self.logger.info('Acquired work for request(s): %s' % requests)
        return work

    def doneWork(self, elementIDs = None, SubscriptionId = None, WorkflowName = None):
        """Mark work as done
        """
        try:
            return self.setStatus('Done', elementIDs = elementIDs, SubscriptionId = SubscriptionId, WorkflowName = WorkflowName)
        except RuntimeError:
            if SubscriptionId:
                self.logger.info("""Done Update: Only some subscription is 
                                    updated Might be the child subscriptions: %s""" 
                                    % elementIDs)
                return elementIDs
            else:
                raise

    def failWork(self, elementIDs, id_type = 'id'):
        """Mark work as failed"""
        try:
            return self.setStatus('Failed', elementIDs, id_type)
        except RuntimeError:
            if id_type == "subscription_id":
                self.logger.info("""Fail update: Only some subscription is 
                                    updated Might be the child subscriptions: %s""" 
                                    % elementIDs)
                return elementIDs
            else:
                raise
        return elementIDs


    def cancelWork(self, elementIDs = None, SubscriptionId = None, WorkflowName = None, elements = None):
        """Cancel work - delete in wmbs, delete from workqueue db, set canceled in inbox
           Elements may be directly provided or determined from series of filter arguments
        """
        if not self.params['LocalQueueFlag']:
            raise RuntimeError, "Not a local queue - can't cancel work"
        if not elements:
            args = {}
            if SubscriptionId:
                args['SubscriptionId'] = SubscriptionId
            if WorkflowName:
                args['RequestName'] = WorkflowName
            elements = self.backend.getElements(elementIDs = elementIDs, **args)

        # if we can talk to wmbs kill the jobs
        if self.params['PopulateFilesets']:
            from WMCore.WorkQueue.WMBSHelper import killWorkflow

            requestNames = set([x['RequestName'] for x in elements])
            self.logger.debug("""Canceling work in wmbs, workflows: %s""" % (requestNames))
            for workflow in requestNames:
                try:
                    myThread = threading.currentThread()
                    myThread.dbi = self.conn.dbi
                    myThread.logger = self.logger
                    killWorkflow(workflow, self.params["JobDumpConfig"],
                                 self.params["BossAirConfig"])
                except RuntimeError:
                    #TODO: Check this logic and improve if possible
                    if SubscriptionId:
                        self.logger.info("""Cancel update: Only some subscription's canceled.
                                    This might be due to a child subscriptions: %s"""
                                    % elementIDs)

            # update parent elements to canceled
            for wf in requestNames:
                inbox_elements = self.backend.getInboxElements(WorkflowName = wf, returnIdOnly = True)
                if not inbox_elements:
                    raise RuntimeError, "Cant find parent for %s" % wf
                self.backend.updateInboxElements(*inbox_elements, Status = 'Canceled')
            # delete elements - no longer need them
            self.backend.deleteElements(*elements)

        return [x.id for x in elements]


    def deleteWorkflows(self, *requests):
        """Delete requests if finished"""
        for request in requests:
            request = self.backend.getInboxElements(elementIDs = [request])
            if len(request) != 1:
                raise RuntimeError, 'Invalid number of requests for %s' % request[0]['RequestName']
            request = request[0]

            if request.inEndState():
                self.logger.info('Deleting request "%s" as it is %s' % (request.id, request['Status']))
                self.backend.deleteElements(request)
            else:
                self.logger.error('Not deleting "%s" as it is %s' % (request.id, request['Status']))

    def queueWork(self, wmspecUrl, request = None, team = None):
        """
        Take and queue work from a WMSpec.

        If request name is provided but doesn't match WMSpec name
        an error is raised.

        If team is provided work will only be available to queue's
        belonging to that team.

        Duplicate specs will be ignored.
        """
        self.logger.info('queueWork() begin queueing "%s"' % wmspecUrl)
        wmspec = WMWorkloadHelper()
        wmspec.load(wmspecUrl)

        # check we haven't already got this work
        try:
            self.backend.getInboxElements(elementIDs = [wmspec.name()])
        except CouchNotFoundError:
            pass
        else:
            self.logger.warning('queueWork(): Ignoring duplicate spec "%s"' % wmspec.name())
            return 1

        if request and request != wmspec.name():
            raise WorkQueueWMSpecError(wmspec, 'Request & workflow name mismatch %s vs %s' % (request, wmspec.name()))

        # Do splitting before we save inbound work to verify the wmspec
        # if the spec fails it won't enter the queue
        inbound = self.backend.createWork(wmspec, team)

        # either we have already split the work or we do that now
        work = self.backend.getElementsForWorkflow(wmspec.name())
        if work:
            self.logger.info('Request "%s" already split - Resuming' % str(wmspec.name()))
        else:
            work = self._splitWork(wmspec, None, inbound['Inputs'], inbound['Mask'])
            self.backend.insertElements(work, parent = inbound) # if this fails, rerunning will pick up here

        self.backend.insertElements([inbound]) # save inbound work to signal we have completed queueing
        return len(work)

    def status(self, status = None, elementIDs = None,
               dictKey = None, syncWithWMBS = False, loadSpec = False,
               **filters):
        """
        Return elements in the queue.

        status, elementIDs & filters are 'AND'ed together to filter elements.
        dictKey returns the output as a dict with the dictKey as the key.
        syncWithWMBS causes elements to be synced with their status in WMBS.
        loadSpec causes the workflow for each spec to be loaded.
        """
        items = self.backend.getElements(status = status,
                                         elementIDs = elementIDs,
                                         loadSpec = loadSpec,
                                         **filters)

        if syncWithWMBS:
            from WMCore.WorkQueue.WMBSHelper import wmbsSubscriptionStatus
            wmbs_status = wmbsSubscriptionStatus(logger = self.logger,
                                                 dbi = self.conn.dbi,
                                                 conn = self.conn.getDBConn(),
                                    transaction = self.conn.existingTransaction())
            for item in items:
                for wmbs in wmbs_status:
                    if item['SubscriptionId'] == wmbs['subscription_id']:
                        item.updateFromSubscription(wmbs)
                        break

        # if dictKey, format as a dict with the appropriate key
        if dictKey:
            tmp = defaultdict(list)
            for item in items:
                tmp[item[dictKey]].append(item)
            items = dict(tmp)
        return items


    def statusInbox(self, status = None, elementIDs = None, dictKey = None, **filters):
        """
        Return elements in the inbox.

        status, elementIDs & filters are 'AND'ed together to filter elements.
        dictKey returns the output as a dict with the dictKey as the key.
        """
        items = self.backend.getInboxElements(status, elementIDs, **filters)

        # if dictKey, given format as a dict with the appropriate key
        if dictKey:
            tmp = defaultdict(list)
            for item in items:
                tmp[item[dictKey]].append(item)
            items = dict(tmp)

        return items


    def updateLocationInfo(self):
        """
        Update locations info for elements.
        """
        if not self.backend.isAvailable():
            self.logger.info('Backend busy or down: skipping location update')
            return 0
        result = self.dataLocationMapper()
        self.backend.recordTaskActivity('location_refresh')
        return result

    def pullWork(self, resources = None):
        """
        Pull work from another WorkQueue to be processed

        If resources passed in get work for them, if not available resources
        from get from wmbs.
        """
        if not self.params['ParentQueueCouchUrl']:
            msg = 'Unable to pull work from parent, ParentQueueCouchUrl not provided'
            self.logger.warning(msg)
            return 0
        if not self.backend.isAvailable() or not self.parent_queue.isAvailable():
            self.logger.info('Backend busy or down: skipping work pull')
            return 0
        if self.params['DrainMode']:
            self.logger.info('Draining queue: skipping work pull')
            return 0

        if not resources:
            # find out available resources from wmbs
            from WMCore.WorkQueue.WMBSHelper import freeSlots
            sites = freeSlots(self.params['QueueDepth'])
            # resources for new work are free wmbs resources minus what we already have queued
            _, resources = self.backend.availableWork(sites)

        if not resources:
            self.logger.info('Not pulling more work. No free slots.')
            return 0

        left_over = self.parent_queue.getElements('Negotiating', returnIdOnly = True,
                                                  ChildQueueUrl = self.params['QueueURL'])
        if left_over:
            self.logger.info('Not pulling more work. Still replicating %d previous units' % len(left_over))
            return 0

        still_processing = self.backend.getInboxElements('Negotiating', returnIdOnly = True)
        if still_processing:
            self.logger.info('Not pulling more work. Still processing %d previous units' % len(still_processing))
            return 0

        self.logger.info("Pull work for sites %s: " % str(resources))

        work, _ = self.parent_queue.availableWork(resources, self.params['Teams'])
        if not work:
            self.logger.info('No available work in parent queue.')
            return 0
        work = self._assignToChildQueue(self.params['QueueURL'],
                                        self.params['WMBSURL'], *work)

        # do this whether we have work or not - other events i.e. cancel may have happened
        self.backend.pullFromParent()
        return len(work)

    def performQueueCleanupActions(self, skipWMBS = False):
        """
        Apply end policies to determine work status & cleanup finished work
        """
        if not self.backend.isAvailable():
            self.logger.info('Backend busy or down: skipping cleanup tasks')
            return

        self.backend.pullFromParent() # Check we are upto date with inbound changes
        self.backend.fixConflicts() # before doing anything fix any conflicts

        wf_to_cancel = [] # record what we did for task_activity
        finished_elements = []

        useWMBS = not skipWMBS and self.params['LocalQueueFlag']
        # Get queue elements grouped by their workflow with updated wmbs progress
        # Cancel if requested, update locally and remove obsolete elements
        for wf, elements in self.status(dictKey = "RequestName", syncWithWMBS = useWMBS).items():
            try:
                parents = self.backend.getInboxElements(RequestName = wf)
                if not parents:
                    raise RuntimeError, "Parent elements not found for %s" % wf

                results = endPolicy(elements, parents, self.params['EndPolicySettings'])
                for result in results:
                    # check for cancellation requests (affects entire workflow)
                    if self.params['LocalQueueFlag'] and result['Status'] == 'CancelRequested':
                        self.cancelWork(elements = elements)
                        wf_to_cancel.append(wf)
                        break

                    parent = result['ParentQueueElement']
                    if parent.modified:
                        self.backend.updateInboxElements(parent.id, **parent.statusMetrics())

                    if result.inEndState():
                        self.backend.deleteElements(*result['Elements'])
                        finished_elements.extend(result['Elements'])
                        continue

                    [self.backend.updateElements(x.id, **x.statusMetrics()) for x in result['Elements'] if x.modified]
            except Exception, ex:
                self.logger.error('Error processing workflow "%s": %s' % (wf, str(ex)))

        msg = 'Finished elements: %s\nCanceled workflows: %s' % (', '.join(["%s (%s)" % (x.id, x['RequestName']) \
                                                                            for x in finished_elements]),
                                                                 ', '.join(wf_to_cancel))
        self.backend.recordTaskActivity('housekeeping', msg)
        self.backend.sendToParent() # update parent queue with new status's


    def _splitWork(self, wmspec, parentQueueId = None,
                   data = None, mask = None, team = None):
        """
        Split work from a parent into WorkQeueueElements.

        If data param supplied use that rather than getting input data from
        wmspec. Used for instance when global splits by Block (avoids having to
        modify wmspec block whitelist - thus all appear as same wf in wmbs)

        mask can be used to specify i.e. event range.
        """
        totalUnits = []

        # split each top level task into constituent work elements
        # get the acdc server and db name
        for topLevelTask in wmspec.taskIterator():
            spec = getWorkloadFromTask(topLevelTask)
            policyName = spec.startPolicy()
            if not policyName:
                raise RuntimeError("WMSpec doesn't define policyName, current value: '%s'" % policyName)

            # update policy parameter
            self.params['SplittingMapping'][policyName].update(args = spec.startPolicyParameters())
            policy = startPolicy(policyName, self.params['SplittingMapping'])
            self.logger.info("Using %s start policy with %s " % (policyName,
                                            self.params['SplittingMapping']))
            units = policy(spec, topLevelTask, data, mask)
            self.logger.info("Queuing %s unit(s): wf: %s for task: %s" % (
                             len(units), spec.name(), topLevelTask.name()))
            totalUnits.extend(units)

        return totalUnits

    def processInboundWork(self):
        """Retrieve work from inbox, split and store
        """
        self.backend.fixConflicts() # db should be consistent

        result = []
        inbound_work = self.backend.getElementsForSplitting()
        for inbound in inbound_work:
            # Check we haven't already split the work
            work = self.backend.getElementsForParent(inbound)

            if not work:
                try: # We haven't split this before, do so now
                    work = self._splitWork(inbound['WMSpec'], inbound.id, inbound['Inputs'], inbound['Mask'])
                except Exception, ex:
                    self.logger.exception('Exception splitting work for wmspec "%s": %s' % (inbound['WMSpec'].name(), str(ex)))
                    continue

                self.backend.insertElements(work, parent = inbound)
            inbound['Status'] = 'Acquired'  # update parent
            self.backend.saveElements(inbound) # if this fails subsequent updates will retry
            result.extend(work)
        requests = ', '.join(list(set(['"%s"' % x['RequestName'] for x in result])))
        if requests:
            self.logger.info('Split work for request(s): %s' % requests)

        return result

    def getWMBSInjectionStatus(self, workflowName = None):
        """
        if the parent queue exist return the result from parent queue.
        other wise return the result from the current queue.
        (In general parent queue always exist when it is called from local queue
        except T1 skim case)
        returns list of [{workflowName: injection status (True or False)}]
        if the workflow is not exist return []
        """
        if self.parent_queue:
            return self.parent_queue.getWMBSInjectStatus(workflowName)
        else:
            return self.backend.getWMBSInjectStatus(workflowName)
