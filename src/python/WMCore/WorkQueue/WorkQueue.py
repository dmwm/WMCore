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
import time
from httplib import HTTPException

from WMCore.Alerts import API as alertAPI

from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB

from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
from WMCore.WorkQueue.Policy.Start import startPolicy
from WMCore.WorkQueue.Policy.End import endPolicy
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueNoMatchingElements
from WMCore.WorkQueue.WorkQueueExceptions import TERMINAL_EXCEPTIONS
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueError
from WMCore.WorkQueue.WorkQueueUtils import get_dbs
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames

from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, getWorkloadFromTask
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.WorkQueue.DataLocationMapper import WorkQueueDataLocationMapper

from WMCore.Database.CMSCouch import CouchNotFoundError, CouchInternalServerError

from WMCore import Lexicon
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Services.ReqMgr.ReqMgr         import ReqMgr
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
    defaults = {'TrackLocationOrSubscription' : 'location'}
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

        # config argument (within params) shall be reference to
        # Configuration instance (will later be checked for presence of "Alert")
        self.config = params.get("Config", None)
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
            try:
                if self.params.get('ParentQueueInboxCouchDBName'):
                    self.parent_queue = WorkQueueBackend(self.params['ParentQueueCouchUrl'].rsplit('/', 1)[0],
                                                         self.params['ParentQueueCouchUrl'].rsplit('/', 1)[1],
                                                         self.params['ParentQueueInboxCouchDBName'])
                else:
                    self.parent_queue = WorkQueueBackend(self.params['ParentQueueCouchUrl'].rsplit('/', 1)[0],
                                                         self.params['ParentQueueCouchUrl'].rsplit('/', 1)[1])
            except IndexError, ex:
                # Probable cause: Someone didn't put the global WorkQueue name in
                # the ParentCouchUrl
                msg =  "Parsing failure for ParentQueueCouchUrl - probably missing dbname in input\n"
                msg += "Exception: %s\n" % str(ex)
                msg += str("ParentQueueCouchUrl: %s\n" % self.params['ParentQueueCouchUrl'])
                self.logger.error(msg)
                raise WorkQueueError(msg)
            self.params['ParentQueueCouchUrl'] = self.parent_queue.queueUrl

        self.params.setdefault("GlobalDBS",
                               "https://cmsweb.cern.ch/dbs/prod/global/DBSReader")
        self.params.setdefault('QueueDepth', 0.5) # when less than this locally
        self.params.setdefault('LocationRefreshInterval', 600)
        self.params.setdefault('FullLocationRefreshInterval', 7200)
        self.params.setdefault('TrackLocationOrSubscription', 'subscription')
        self.params.setdefault('ReleaseIncompleteBlocks', False)
        self.params.setdefault('ReleaseRequireSubscribed', True)
        self.params.setdefault('PhEDExEndpoint', None)
        self.params.setdefault('PopulateFilesets', True)
        self.params.setdefault('LocalQueueFlag', True)
        self.params.setdefault('QueueRetryTime', 86400)
        self.params.setdefault('stuckElementAlertTime', 172800)
        self.params.setdefault('reqmgrCompleteGraceTime', 604800)
        self.params.setdefault('cancelGraceTime', 86400)

        self.params.setdefault('JobDumpConfig', None)
        self.params.setdefault('BossAirConfig', None)

        self.params['QueueURL'] = self.backend.queueUrl # url this queue is visible on
                                    # backend took previous QueueURL and sanitized it
        self.params.setdefault('WMBSUrl', None) # this will only be set on local Queue
        if self.params.get('WMBSUrl'):
            self.params['WMBSUrl'] = Lexicon.sanitizeURL(self.params['WMBSUrl'])['url']
        self.params.setdefault('Teams', [])
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

        # initialize alerts sending client (self.sendAlert() method)
        # usage: self.sendAlert(levelNum, msg = msg) ; level - integer 1 .. 10
        #    1 - 4 - lower levels ; 5 - 10 higher levels
        preAlert, self.alertSender = \
            alertAPI.setUpAlertsMessaging(self, compName = "WorkQueueManager")
        self.sendAlert = alertAPI.getSendAlert(sender = self.alertSender,
                                               preAlert = preAlert)

        self.logger.debug("WorkQueue created successfully")

    def __len__(self):
        """Returns number of Available elements in queue"""
        return self.backend.queueLength()

    def __del__(self):
        """
        Unregister itself with Alert Receiver.
        The registration happened in the constructor when initializing.

        """
        if self.alertSender:
            self.alertSender.unregister()

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

        if status == 'Canceled': # Cancel needs special actions
            return self.cancelWork(elementIDs, SubscriptionId, WorkflowName)

        args = {}
        if SubscriptionId:
            args['SubscriptionId'] = SubscriptionId
        if WorkflowName:
            args['RequestName'] = WorkflowName

        affected = self.backend.getElements(elementIDs = elementIDs, **args)
        if not affected:
            raise WorkQueueNoMatchingElements, "No matching elements"

        for x in affected:
            x['Status'] = status
        elements = self.backend.saveElements(*affected)
        if len(affected) != len(elements):
            raise RuntimeError, "Some elements not updated, see log for details"

        return elements

    def setPriority(self, newpriority, *workflowNames):
        """
        Update priority for a workflow, throw exception if no elements affected
        """
        self.logger.info("Priority change request to %s for %s" % (newpriority, str(workflowNames)))
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
        self.logger.info("Resetting elements %s" % str(ids))
        try:
            iter(ids)
        except TypeError:
            ids = [ids]

        return self.backend.updateElements(*ids, Status = 'Available',
                                           ChildQueueUrl = None, WMBSUrl = None)

    def getWork(self, jobSlots, siteJobCounts):
        """
        Get available work from the queue, inject into wmbs & mark as running

        jobSlots is dict format of {site: estimateJobSlot}
        of the resources to get work for.

        siteJobCounts is a dict format of {site: {prio: jobs}}
        """
        results = []
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: skipping fetching of work')
            return results
        matches, _, _ = self.backend.availableWork(jobSlots, siteJobCounts)

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
                    blockName, dbsBlock = self._getDBSBlock(match, wmspec)

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
            fileLists = acdc.getChunkFiles(acdcInfo['collection'],
                                           acdcInfo['fileset'],
                                           splitedBlockName['Offset'],
                                           splitedBlockName['NumOfFiles'],
                                           user = wmspec.getOwner().get("name"),
                                           group = wmspec.getOwner().get("group"))
            block = {}
            block["Files"] = fileLists
            if wmspec.locationDataSourceFlag():
                seElements = []
                for cmsSite in match['Inputs'].values()[0]: #TODO: Allow more than one
                    ses = self.SiteDB.cmsNametoSE(cmsSite)
                    seElements.extend(ses)
                seElements = list(set(seElements))
                for fileInfo in block["Files"]:
                    fileInfo['locations'] = seElements
            return blockName, block
        else:
            dbs = get_dbs(match['Dbs'])
            if wmspec.getTask(match['TaskName']).parentProcessingFlag():
                dbsBlockDict = dbs.getFileBlockWithParents(blockName)
            else:
                dbsBlockDict = dbs.getFileBlock(blockName)

            if wmspec.locationDataSourceFlag():
                blockInfo = dbsBlockDict[blockName]
                seElements = []
                for cmsSite in match['Inputs'].values()[0]: #TODO: Allow more than one
                    ses = self.SiteDB.cmsNametoSE(cmsSite)
                    seElements.extend(ses)
                seElements = list(set(seElements))
                blockInfo['StorageElements'] = seElements
        return blockName, dbsBlockDict[blockName]

    def _wmbsPreparation(self, match, wmspec, blockName, dbsBlock):
        """Inject data into wmbs and create subscription.
        """
        from WMCore.WorkQueue.WMBSHelper import WMBSHelper
        self.logger.info("Adding WMBS subscription for %s" % match['RequestName'])

        mask = match['Mask']
        wmbsHelper = WMBSHelper(wmspec, match['TaskName'], blockName, mask, self.params['CacheDir'])

        sub, match['NumOfFilesAdded'] = wmbsHelper.createSubscriptionAndAddFiles(block = dbsBlock)
        self.logger.info("Created top level subscription %s for %s with %s files" % (sub['id'],
                                                                                     match['RequestName'],
                                                                                     match['NumOfFilesAdded']))
        # update couch with wmbs subscription info
        match['SubscriptionId'] = sub['id']
        match['Status'] = 'Running'
        # do update rather than save to avoid conflicts from other thread writes
        self.backend.updateElements(match.id, Status = 'Running', SubscriptionId = sub['id'],
                                    NumOfFilesAdded = match['NumOfFilesAdded'])

        return sub

    def addNewFilesToOpenSubscriptions(self, *elements):
        """Inject new files to wmbs for running elements that have new files.
            Assumes elements are from the same workflow"""
        if not self.params['LocalQueueFlag']:
            return
        wmspec = None
        for ele in elements:
            if not ele.isRunning() or not ele['SubscriptionId'] or not ele:
                continue
            if not ele['Inputs'] or not ele['OpenForNewData']:
                continue
            if not wmspec:
                wmspec = self.backend.getWMSpec(ele['RequestName'])
            blockName, dbsBlock = self._getDBSBlock(ele, wmspec)
            if ele['NumOfFilesAdded'] != len(dbsBlock['Files']):
                self.logger.info("Adding new files to open block %s (%s)" % (blockName, ele.id))
                from WMCore.WorkQueue.WMBSHelper import WMBSHelper
                wmbsHelper = WMBSHelper(wmspec, ele['TaskName'], blockName, ele['Mask'], self.params['CacheDir'])
                ele['NumOfFilesAdded'] += wmbsHelper.createSubscriptionAndAddFiles(block = dbsBlock)[1]
                self.backend.updateElements(ele.id, NumOfFilesAdded = ele['NumOfFilesAdded'])
            if dbsBlock['IsOpen'] != ele['OpenForNewData']:
                self.logger.info("Closing open block %s (%s)" % (blockName, ele.id))
                self.backend.updateInboxElements(ele['ParentQueueId'], OpenForNewData = dbsBlock['IsOpen'])
                self.backend.updateElements(ele.id, OpenForNewData = dbsBlock['IsOpen'])
                ele['OpenForNewData'] = dbsBlock['IsOpen']

    def _assignToChildQueue(self, queue, *elements):
        """Assign work from parent to queue"""
        for ele in elements:
            ele['Status'] = 'Negotiating'
            ele['ChildQueueUrl'] = queue
            ele['ParentQueueUrl'] = self.params['ParentQueueCouchUrl']
            ele['WMBSUrl'] = self.params["WMBSUrl"]
        work = self.parent_queue.saveElements(*elements)
        requests = ', '.join(list(set(['"%s"' % x['RequestName'] for x in work])))
        self.logger.info('Acquired work for request(s): %s' % requests)
        return work

    def doneWork(self, elementIDs = None, SubscriptionId = None, WorkflowName = None):
        """Mark work as done
        """
        return self.setStatus('Done', elementIDs = elementIDs,
                              SubscriptionId = SubscriptionId,
                              WorkflowName = WorkflowName)

    def cancelWork(self, elementIDs = None, SubscriptionId = None, WorkflowName = None, elements = None):
        """Cancel work - delete in wmbs, delete from workqueue db, set canceled in inbox
           Elements may be directly provided or determined from series of filter arguments
        """
        if not elements:
            args = {}
            if SubscriptionId:
                args['SubscriptionId'] = SubscriptionId
            if WorkflowName:
                args['RequestName'] = WorkflowName
            elements = self.backend.getElements(elementIDs = elementIDs, **args)

        # take wf from args in case no elements exist for workflow (i.e. work was negotiating)
        requestNames = set([x['RequestName'] for x in elements]) | set([wf for wf in [WorkflowName] if wf])
        if not requestNames:
            return []
        inbox_elements = []
        for wf in requestNames:
            inbox_elements.extend(self.backend.getInboxElements(WorkflowName = wf))

        # if local queue, kill jobs, update parent to Canceled and delete elements
        if self.params['LocalQueueFlag']:
            # if we can talk to wmbs kill the jobs
            if self.params['PopulateFilesets']:
                self.logger.info("""Canceling work for workflow(s): %s""" % (requestNames))
                from WMCore.WorkQueue.WMBSHelper import killWorkflow
                for workflow in requestNames:
                    try:
                        myThread = threading.currentThread()
                        myThread.dbi = self.conn.dbi
                        myThread.logger = self.logger
                        killWorkflow(workflow, self.params["JobDumpConfig"],
                                     self.params["BossAirConfig"])
                    except Exception, ex:
                        self.logger.error('Aborting %s wmbs subscription failed: %s' % (workflow, str(ex)))
            # Don't update as fails sometimes due to conflicts (#3856)
            [x.load().__setitem__('Status', 'Canceled') for x in inbox_elements if x['Status'] != 'Canceled']
            updated_inbox_ids = [x.id for x in self.backend.saveElements(*inbox_elements)]
            # delete elements - no longer need them
            self.backend.deleteElements(*[x for x in elements if x['ParentQueueId'] in updated_inbox_ids])

        # if global queue, update non-acquired to Canceled, update parent to CancelRequested
        else:
            # Cancel in global if work has not been passed to a child queue
            elements_to_cancel = [x for x in elements if not x['ChildQueueUrl'] and x['Status'] != 'Canceled']
            # ensure all elements receive cancel request, covers case where initial cancel request missed some elements
            # without this elements may avoid the cancel and not be cleared up till they finish
            elements_not_requested = [x for x in elements if x['ChildQueueUrl'] and (x['Status'] != 'CancelRequested' and not x.inEndState())]
            if elements_to_cancel or elements_not_requested:
                self.logger.info("""Canceling work for workflow(s): %s""" % (requestNames))
                self.logger.info("Canceling element(s) %s" % str([x.id for x in elements]))
            self.backend.updateElements(*[x.id for x in elements_to_cancel], Status = 'Canceled')
            self.backend.updateInboxElements(*[x.id for x in inbox_elements if x['Status'] != 'CancelRequested' and not x.inEndState()], Status = 'CancelRequested')
            # if we haven't had any updates for a while assume agent is dead and move to canceled
            if self.params.get('cancelGraceTime', -1) > 0 and elements:
                last_update = max([float(x.updatetime) for x in elements])
                if (time.time() - last_update) > self.params['cancelGraceTime']:
                    self.logger.info("%s cancelation has stalled, mark as finished" % elements[0]['RequestName'])
                    # Don't update as fails sometimes due to conflicts (#3856)
                    [x.load().__setitem__('Status', 'Canceled') for x in elements if not x.inEndState()]
                    self.backend.saveElements(*[x for x in elements if not x.inEndState()])
            else:
                # Don't update as fails sometimes due to conflicts (#3856)
                [x.load().__setitem__('Status', 'CancelRequested') for x in elements_not_requested]
                self.backend.saveElements(*elements_not_requested)

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
                self.logger.debug('Not deleting "%s" as it is %s' % (request.id, request['Status']))

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

        if request: # validate request name
            try:
                Lexicon.requestName(request)
            except Exception, ex: # can throw many errors e.g. AttributeError, AssertionError etc.
                error = WorkQueueWMSpecError(wmspec, "Request name validation error: %s" % str(ex))
                raise error
            if request != wmspec.name():
                raise WorkQueueWMSpecError(wmspec, 'Request & workflow name mismatch %s vs %s' % (request, wmspec.name()))

        # Either pull the existing inbox element or create a new one.
        try:
            inbound = self.backend.getInboxElements(elementIDs = [wmspec.name()], loadSpec = True)
            self.logger.info('Resume splitting of "%s"' % wmspec.name())
        except CouchNotFoundError:
            inbound = [self.backend.createWork(wmspec, Status = 'Negotiating',
                                              TeamName = team, WMBSUrl = self.params["WMBSUrl"])]
            self.backend.insertElements(inbound)

        work = self.processInboundWork(inbound, throw = True)
        return len(work)

    def addWork(self, requestName):
        """
        Check and add new elements to an existing running request,
        if supported by the start policy.
        """
        self.logger.info('addWork() checking "%s"' % requestName)
        inbound = None
        try:
            inbound = self.backend.getInboxElements(elementIDs = [requestName], loadSpec = True)
        except CouchNotFoundError:
            #This shouldn't happen, the request is in running-open therefore it must exist in the inbox
            self.logger.error('Can not find request %s for work addition' % requestName)
            return 0

        work = []
        if inbound:
            work = self.processInboundWork(inbound, throw = True, continuous = True)
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

    def pullWork(self, resources = None, continuousReplication = True):
        """
        Pull work from another WorkQueue to be processed

        If resources passed in get work for them, if not available resources
        from get from wmbs.
        """
        
        # do this whether we have work or not - other events i.e. cancel may have happened
        replicationFlag = self.backend.checkReplicationStatus(continuous = continuousReplication)
        if replicationFlag:
            self.logger.info("Replication is set for LocalQueue")
        
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

        jobCounts = {}
        if not resources:
            # find out available resources from wmbs
            from WMCore.WorkQueue.WMBSHelper import freeSlots
            thresholds, jobCounts = freeSlots(self.params['QueueDepth'], knownCmsSites = cmsSiteNames())
            # resources for new work are free wmbs resources minus what we already have queued
            _, resources, jobCounts = self.backend.availableWork(thresholds, jobCounts)

        if not resources:
            self.logger.info('Not pulling more work. No free slots.')
            return 0

        left_over = self.parent_queue.getElements('Negotiating', returnIdOnly = True,
                                                  ChildQueueUrl = self.params['QueueURL'])
        if left_over:
            self.logger.info('Not pulling more work. Still replicating %d previous units, ids:\n%s' % (
                                                                        len(left_over), left_over))

            return 0

        still_processing = self.backend.getInboxElements('Negotiating', returnIdOnly = True)
        if still_processing:
            self.logger.info('Not pulling more work. Still processing %d previous units' % len(still_processing))
            return 0

        self.logger.info("Pull work for sites %s: " % str(resources))

        work, _, _ = self.parent_queue.availableWork(resources, jobCounts, self.params['Teams'])

        if not work:
            self.logger.info('No available work in parent queue.')
            return 0
        work = self._assignToChildQueue(self.params['QueueURL'], *work)

        return len(work)

    def closeWork(self, *workflows):
        """
        Global queue service that looks for the inbox elements that are still running open
        and checks whether they should be closed already. If a list of workflows
        is specified then those workflows are closed regardless of their current status.
        An element is closed automatically when one of the following conditions holds true:
        - The StartPolicy doesn't define a OpenRunningTimeout or this delay is set to 0
        - A period longer than OpenRunningTimeout has passed since the last child element was created or an open block was found
          and the StartPolicy newDataAvailable function returns False.
        It also checks if new data is available and updates the inbox element
        """

        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: Can not close work at this time')
            return

        if self.params['LocalQueueFlag']:
            return # GlobalQueue-only service

        if workflows:
            workflowsToClose = workflows
        else:
            workflowsToCheck = self.backend.getInboxElements(OpenForNewData = True)
            workflowsToClose = []
            currentTime = time.time()
            for element in workflowsToCheck:
                # Easy check, close elements with no defined OpenRunningTimeout
                policy = element.get('StartPolicy', {})
                openRunningTimeout = policy.get('OpenRunningTimeout', 0)
                if not openRunningTimeout:
                    # Closing, no valid OpenRunningTimeout available
                    workflowsToClose.append(element.id)
                    continue

                # Check if new data is currently available
                skipElement = False
                spec = self.backend.getWMSpec(element.id)
                for topLevelTask in spec.taskIterator():
                    policyName = spec.startPolicy()
                    if not policyName:
                        raise RuntimeError("WMSpec doesn't define policyName, current value: '%s'" % policyName)

                    policyInstance = startPolicy(policyName, self.params['SplittingMapping'])
                    if not policyInstance.supportsWorkAddition():
                        continue
                    if policyInstance.newDataAvailable(topLevelTask, element):
                        skipElement = True
                        self.backend.updateInboxElements(element.id, TimestampFoundNewData = currentTime)
                        break
                if skipElement:
                    continue

                # Check if the delay has passed
                newDataFoundTime = element.get('TimestampFoundNewData', 0)
                childrenElements = self.backend.getElementsForParent(element)
                lastUpdate = float(max(childrenElements, key = lambda x: x.timestamp).timestamp)
                if (currentTime - max(newDataFoundTime, lastUpdate)) > openRunningTimeout:
                    workflowsToClose.append(element.id)

        msg = 'No workflows to close.\n'
        if workflowsToClose:
            try:
                self.backend.updateInboxElements(*workflowsToClose, OpenForNewData = False)
                msg = 'Closed workflows : %s.\n' % ', '.join(workflows)
            except CouchInternalServerError, ex:
                msg = 'Failed to close workflows. Error was CouchInternalServerError.'
                self.logger.error(msg)
                self.logger.error('Error message: %s' % str(ex))
                raise
            except Exception, ex:
                msg = 'Failed to close workflows. Generic exception caught.'
                self.logger.error(msg)
                self.logger.error('Error message: %s' % str(ex))

        self.backend.recordTaskActivity('workclosing', msg)

        return workflowsToClose

    def performQueueCleanupActions(self, skipWMBS = False):
        """
        Apply end policies to determine work status & cleanup finished work
        """
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: skipping cleanup tasks')
            return

        if self.params['LocalQueueFlag']:
            self.backend.checkReplicationStatus() # Check any replication error and fix it.
            self.backend.fixConflicts() # before doing anything fix any conflicts

        wf_to_cancel = [] # record what we did for task_activity
        finished_elements = []

        useWMBS = not skipWMBS and self.params['LocalQueueFlag']
        # Get queue elements grouped by their workflow with updated wmbs progress
        # Cancel if requested, update locally and remove obsolete elements
        for wf in self.backend.getWorkflows(includeInbox = True, includeSpecs = True):
            try:
                elements = self.status(RequestName = wf, syncWithWMBS = useWMBS)
                parents = self.backend.getInboxElements(RequestName = wf)

                # check for left overs from past work where cleanup needed
                if elements and not parents:
                    self.logger.info("Removing orphaned elements for %s" % wf)
                    self.backend.deleteElements(*elements)
                    continue
                if not elements and not parents:
                    self.logger.info("Removing orphaned workflow %s" % wf)
                    try:
                        self.backend.db.delete_doc(wf)
                    except CouchNotFoundError:
                        pass
                    continue

                self.logger.debug("Queue status follows:")
                results = endPolicy(elements, parents, self.params['EndPolicySettings'])
                for result in results:
                    self.logger.debug("Request %s, Status %s, Full info: %s" % (result['RequestName'], result['Status'], result))

                    # check for cancellation requests (affects entire workflow)
                    if result['Status'] == 'CancelRequested':
                        canceled = self.cancelWork(WorkflowName = wf)
                        if canceled: # global wont cancel if work in child queue
                            wf_to_cancel.append(wf)
                            break
                    elif result['Status'] == 'Negotiating':
                        self.logger.debug("Waiting for %s to finish splitting" % wf)
                        continue

                    parent = result['ParentQueueElement']
                    if parent.modified:
                        self.backend.saveElements(parent)

                    if result.inEndState():
                        if elements:
                            self.logger.info("Request %s finished (%s)" % (result['RequestName'], parent.statusMetrics()))
                            self.backend.deleteElements(*result['Elements'])
                            finished_elements.extend(result['Elements'])
                        else:
                            self.logger.info('Waiting for parent queue to delete "%s"' % result['RequestName'])
                        continue

                    self.addNewFilesToOpenSubscriptions(*elements)

                    updated_elements = [x for x in result['Elements'] if x.modified]
                    for x in updated_elements:
                        self.logger.debug("Updating progress %s (%s): %s" % (x['RequsetName'], x.id, x.statusMetrics()))
                    if not updated_elements and (float(parent.updatetime) + self.params['stuckElementAlertTime']) < time.time():
                        self.sendAlert(5, msg = 'Element for %s stuck for 24 hours.' % wf)
                    [self.backend.updateElements(x.id, **x.statusMetrics()) for x in updated_elements]
            except Exception, ex:
                self.logger.error('Error processing workflow "%s": %s' % (wf, str(ex)))

        msg = 'Finished elements: %s\nCanceled workflows: %s' % (', '.join(["%s (%s)" % (x.id, x['RequestName']) \
                                                                            for x in finished_elements]),
                                                                 ', '.join(wf_to_cancel))
        self.backend.recordTaskActivity('housekeeping', msg)
        self.backend.checkReplicationStatus() # update parent queue with new status's

    def _splitWork(self, wmspec, parentQueueId = None,
                   data = None, mask = None, team = None,
                   inbound = None, continuous = False):
        """
        Split work from a parent into WorkQeueueElements.

        If data param supplied use that rather than getting input data from
        wmspec. Used for instance when global splits by Block (avoids having to
        modify wmspec block whitelist - thus all appear as same wf in wmbs)

        mask can be used to specify i.e. event range.

        The inbound and continous parameters are used to split
        and already split inbox element.
        """
        totalUnits = []
        totalToplevelJobs = 0
        totalEvents = 0
        totalLumis = 0
        totalFiles = 0
        # split each top level task into constituent work elements
        # get the acdc server and db name
        for topLevelTask in wmspec.taskIterator():
            spec = getWorkloadFromTask(topLevelTask)
            policyName = spec.startPolicy()
            if not policyName:
                raise RuntimeError("WMSpec doesn't define policyName, current value: '%s'" % policyName)

            policy = startPolicy(policyName, self.params['SplittingMapping'])
            if not policy.supportsWorkAddition() and continuous:
                # Can't split further with a policy that doesn't allow it
                continue
            if continuous:
                policy.modifyPolicyForWorkAddition(inbound)
            self.logger.info('Splitting %s with policy %s params = %s' % (topLevelTask.getPathName(),
                                                policyName, self.params['SplittingMapping']))
            units, rejectedWork = policy(spec, topLevelTask, data, mask)
            for unit in units:
                msg = 'Queuing element %s for %s with %d job(s) split with %s' % (unit.id,
                                                unit['Task'].getPathName(), unit['Jobs'], policyName)
                if unit['Inputs']:
                    msg += ' on %s' % unit['Inputs'].keys()[0]
                if unit['Mask']:
                    msg += ' on events %d-%d' % (unit['Mask']['FirstEvent'], unit['Mask']['LastEvent'])
                self.logger.info(msg)
                totalToplevelJobs += unit['Jobs']
                totalEvents += unit['NumberOfEvents']
                totalLumis += unit['NumberOfLumis']
                totalFiles += unit['NumberOfFiles']
            totalUnits.extend(units)

        return (totalUnits, {'total_jobs': totalToplevelJobs, 
                             'input_events': totalEvents, 
                             'input_lumis': totalLumis, 
                             'input_num_files': totalFiles}, rejectedWork)

    def processInboundWork(self, inbound_work = None, throw = False, continuous = False):
        """Retrieve work from inbox, split and store
        If request passed then only process that request
        """
        if self.params['LocalQueueFlag']:
            self.logger.info("fixing conflict...")
            self.backend.fixConflicts() # db should be consistent

        result = []
        if not inbound_work and continuous:
            # This is not supported
            return result
        if not inbound_work:
            inbound_work = self.backend.getElementsForSplitting()
        for inbound in inbound_work:
            # Check we haven't already split the work, unless it's continuous processing
            work = self.backend.getElementsForParent(inbound)
            try:
                if work and not continuous:
                    self.logger.info('Request "%s" already split - Resuming' % inbound['RequestName'])
                else:
                    work, totalStats, rejectedWork = self._splitWork(inbound['WMSpec'], None, data = inbound['Inputs'],
                                                                     mask = inbound['Mask'], inbound = inbound, continuous = continuous)

                    # save inbound work to signal we have completed queueing
                    self.backend.insertElements(work, parent = inbound) # if this fails, rerunning will pick up here

                    if not continuous:
                        # Update to Acquired when it's the first processing of inbound work
                        self.backend.updateInboxElements(inbound.id, Status = 'Acquired')

                    # store the inputs in the global queue inbox workflow element
                    if not self.params.get('LocalQueueFlag'):
                        processedInputs = []
                        for unit in work:
                            processedInputs.extend(unit['Inputs'].keys())
                        if processedInputs or rejectedWork:
                            chunkSize = 20
                            chunkProcessed = processedInputs[:chunkSize]
                            chunkRejected = rejectedWork[:chunkSize]
                            # TODO:Make this a POST update, instead of several PUT
                            while processedInputs or rejectedWork:
                                self.backend.updateInboxElements(inbound.id, ProcessedInputs = chunkProcessed,
                                                                 RejectedInputs = chunkRejected, options = {'incremental' : True})
                                processedInputs = processedInputs[chunkSize:]
                                rejectedWork = rejectedWork[chunkSize:]
                                chunkProcessed = processedInputs[:chunkSize]
                                chunkRejected = rejectedWork[:chunkSize]
                    
                    # update request mgr couch doc
                    if not self.params.get('LocalQueueFlag'):
                        # only update global stats for global queue
                        try:
                            # add the total work on wmstat summary or add the recently split work
                            reqmgrSvc = ReqMgr(self.params.get('ReqMgrServiceURL'))
                            reqmgrSvc.updateRequestStats(inbound['WMSpec'].name(), totalStats)
                        except HTTPException, httpEx:
                            msg = "status: %s, reason: %s" % (httpEx.status, httpEx.reason)
                            self.logger.error('Error publishing %s to Request Mgr for %s: %s' % (totalStats,
                                                                        inbound['RequestName'], msg))
                        except Exception, ex:
                            self.logger.error('Error publishing %s to Request Mgr for %s: %s' % (totalStats,
                                                                        inbound['RequestName'], str(ex)))
                    
                    #TODO: remove this when reqmgr is dropped
                    if not self.params.get('LocalQueueFlag') and self.params.get('WMStatsCouchUrl'):
                        # only update global stats for global queue
                        try:
                            # add the total work on wmstat summary or add the recently split work
                            wmstatSvc = WMStatsWriter(self.params.get('WMStatsCouchUrl'))
                            wmstatSvc.insertTotalStats(inbound['WMSpec'].name(), totalStats)
                        except HTTPException, httpEx:
                            msg = "status: %s, reason: %s" % (httpEx.status, httpEx.reason)
                            self.logger.error('Error publishing %s to Request Mgr for %s: %s' % (totalStats,
                                                                        inbound['RequestName'], msg))
                        except Exception, ex:
                            self.logger.error('Error publishing %s to WMStats for %s: %s' % (totalStats,
                                                                            inbound['RequestName'], str(ex)))

            except TERMINAL_EXCEPTIONS, ex:
                if not continuous:
                    # Only fail on first splitting
                    self.logger.info('Failing workflow "%s": %s' % (inbound['RequestName'], str(ex)))
                    self.backend.updateInboxElements(inbound.id, Status = 'Failed')
                    if throw:
                        raise
            except Exception, ex:
                if continuous:
                    continue
                # if request has been failing for too long permanently fail it.
                # last update time was when element was assigned to this queue
                if (float(inbound.updatetime) + self.params['QueueRetryTime']) < time.time():
                    self.logger.info('Failing workflow "%s" as not queued in %d secs: %s' % (inbound['RequestName'],
                                                                                             self.params['QueueRetryTime'],
                                                                                             str(ex)))
                    self.backend.updateInboxElements(inbound.id, Status = 'Failed')
                else:
                    self.logger.info('Exception splitting work for wmspec "%s": %s' % (inbound['RequestName'], str(ex)))
                if throw:
                    raise
                continue
            else:
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
        
