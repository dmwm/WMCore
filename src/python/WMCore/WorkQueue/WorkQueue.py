#!/usr/bin/env python
"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing

WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

import time
import os
import types
import pickle
import threading
try:
    from collections import defaultdict
except (NameError, ImportError):
    pass

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS


from WMCore.Services.PhEDEx.PhEDEx import PhEDEx
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON as SiteDB
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.Policy.Start import startPolicy
from WMCore.WorkQueue.Policy.End import endPolicy
from WMCore.WorkQueue.WorkQueueExceptions import WorkQueueWMSpecError
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.Services.DBS.DBSReader import DBSReader
from DBSAPI.dbsApiException import DbsConfigurationError
from WMCore.WorkQueue.DataLocationMapper import WorkQueueDataLocationMapper

from WMCore.WMSpec.Persistency import PersistencyHelper
#TODO: Scale test
#TODO: Decide whether to move/refactor db functions
#TODO: What about sending messages to component to handle almost live status updates

#  //
# // Convenience constructor functions
#//
def globalQueue(logger = None, dbi = None, **kwargs):
    """Convenience method to create a WorkQueue suitable for use globally
    """
    defaults = {'SplitByBlock' : False,
                'PopulateFilesets' : False,
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
        self.dbsHelpers = {}
        self.remote_queues = {}
        self.lastLocationUpdate = 0
        self.lastFullResync = 0
        self.lastReportToParent = 0
        self.lastFullReportToParent = 0
        self.parent_queue = None
        self.params = params
        #TODO: set correct default global dbs 
        self.params.setdefault("GlobalDBS",
                               "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet")
        self.params.setdefault('ParentQueue', None) # Get more work from here
        self.params.setdefault('QueueDepth', 2) # when less than this locally
        self.params.setdefault('ItemWeight', 0.01) # Queuing time weighted avg
        self.params.setdefault('LocationRefreshInterval', 600)
        self.params.setdefault('FullLocationRefreshInterval', 7200)
        self.params.setdefault('TrackLocationOrSubscription', 'subscription')
        self.params.setdefault('ReleaseIncompleteBlocks', False)
        self.params.setdefault('ReleaseRequireSubscribed', True)
        self.params.setdefault('PhEDExEndpoint', None)
        self.params.setdefault('PopulateFilesets', True)
        self.params.setdefault('LocalQueueFlag', True)

        #This two params needs to be set for resubmission to work
        self.params.setdefault('CouchURL', None)
        self.params.setdefault('ACDCDB', None)
        #TODO: current directory as a default directory might not be a best choice.
        # Don't know where else though 
        self.params.setdefault('CacheDir', os.path.join(os.getcwd(), 'wf'))
        self.params.setdefault('NegotiationTimeout', 3600)
        self.params.setdefault('QueueURL', None) # url this queue is visible on
        self.params.setdefault('FullReportInterval', 300)
        self.params.setdefault('ReportInterval', 300)
        self.params.setdefault('Teams', [''])
        self.params.setdefault('IgnoreDuplicates', True)

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

        if self.params['CacheDir']:
            try:
                os.makedirs(self.params['CacheDir'])
            except OSError:
                pass

        if self.params['ParentQueue'] is not None and not self.params['QueueURL']:
            raise RuntimeError, "ParentQueue defined but not QueueURL"
        if self.params['ParentQueue'] is not None:
            self.parent_queue = self._get_remote_queue(self.params['ParentQueue'])

        self.dbsHelpers.update(self.params.get('DBSReaders', {}))
        if self.params.get('GlobalDBS'):
            self._get_dbs(self.params["GlobalDBS"])

        if type(self.params['Teams']) in types.StringTypes:
            self.params['Teams'] = [x.strip() for x in \
                                    self.params['Teams'].split(',')]

        self.dataLocationMapper = WorkQueueDataLocationMapper(self.logger, self.dbi,
                                                              dbses = self.dbsHelpers,
                                                              phedex = self.phedexService,
                                                              sitedb = self.SiteDB,
                                                              locationFrom = self.params['TrackLocationOrSubscription'],
                                                              incompleteBlocks = self.params['ReleaseIncompleteBlocks'],
                                                              requireBlocksSubscribed = not self.params['ReleaseIncompleteBlocks'],
                                                              fullRefreshInterval = self.params['FullLocationRefreshInterval'],
                                                              updateIntervalCoarseness = self.params['LocationRefreshInterval'])


    #  //
    # // External API
    #//

    def __len__(self):
        """Returns number of Available elements in queue"""
        items = self.daofactory(classname = "WorkQueueElement.CountElements")
        return items.execute('Available', conn = self.getDBConn(),
                                 transaction = self.existingTransaction())

    def setStatus(self, status, ids, id_type = 'id', source = None):
        """
        _setStatus_, throws an exception if no elements are updated

        id_type should be onle of ['parent_queue_id', 'id',
                          'subscription_id', 'request_name']

        @param source - where a status update came from (remote queue)
                      - already knows the status so don't send it
        """
        try:
            iter(ids)
            if type(ids) in types.StringTypes:
                raise TypeError
        except TypeError:
            ids = [ids]

        with self.transactionContext():
            updateAction = self.daofactory(classname =
                                           "WorkQueueElement.UpdateStatus")
            affected = updateAction.execute(status, ids, id_type, source,
                                        conn = self.getDBConn(),
                                        transaction = self.existingTransaction())

            if not affected:
                raise RuntimeError, "Status not changed: No matching elements"

            if self.params['LocalQueueFlag']:
                if status == 'Canceled':
                    requestNameAction = self.daofactory(classname =
                                           "WorkQueueElement.GetRequestNamesByIDs")
                    requestNames = requestNameAction.execute(ids, id_type,
                                               conn = self.getDBConn(),
                                               transaction = self.existingTransaction())
                    self.logger.debug("""Canceling work in wmbs
                                        Workflows: %s""" % (requestNames))

                    from WMCore.WorkQueue.WMBSHelper import killWorkflow
                    for workflow in set(requestNames):
                        # JobDumpConfig and BossAirConfig are needed to be passed
                        # correctly to make this work there is no default value for
                        # these. Whoever instantiate the localqueue need to get these
                        # value from WMAgentConfig.py and set it correctly
                        # only needed in this specific call
                        myThread = threading.currentThread()
                        myThread.dbi = self.dbi
                        myThread.logger = self.logger 
                        killWorkflow(workflow, self.params["JobDumpConfig"],
                                     self.params["BossAirConfig"])

        #TODO: Do we need to message parents/children here?
        # Would be quicker than waiting for the next status updates
        # but would need listening services and firewall holes etc.

    def setProgress(self, workQueueEle, id = 'Id'):
        """Update an elements processing progress.

        id may be be ParentQueueId if passed a WorkQueueElementResult
        """
        updateAction = self.daofactory(classname =
                                       "WorkQueueElement.UpdateProgress")
        ids = [workQueueEle[id]]
        id_type = 'id'
        affected = updateAction.execute(ids, workQueueEle, id_type,
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        if not affected:
            raise RuntimeError, "Progress not changed: No matching elements"

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

    def setReqMgrUpdate(self, when, *ids):
        """Set ReqMgr update time to when for ids"""
        updateAction = self.daofactory(classname = "WorkQueueElement.UpdateReqMgr")
        affected = updateAction.execute(when, ids,
                             conn = self.getDBConn(),
                             transaction = self.existingTransaction())
        if not affected:
            raise RuntimeError, "ReqMgr status not changed: No matching elements"

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

        return self.setStatus('Available', ids,
                              source = 'Reset to original state')

    def getWork(self, siteJobs, pullingQueueUrl = None, team = None):
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
        subResults = []
        matches, unmatched = self._match(siteJobs, team)

        # if talking to a child and have resources left get work from parent
        if pullingQueueUrl and unmatched and self.params['ParentQueue']:
            self.logger.debug('getWork() asking %s for work' % self.params['ParentQueue'])
            try:
                #TODO: Add a timeout thats shorter than normal
                if self.pullWork(unmatched):
                    matches, _ = self._match(siteJobs, team)
            except RuntimeError, ex:
                msg = "Error contacting parent queue %s: %s"
                self.logger.error(msg % (self.params['ParentQueue'], str(ex)))
        wmspecInfoAction = self.daofactory(classname = "WMSpec.GetWMSpecInfo")
        
        wmspecCache = {}
        for match in matches:
            wmspecInfo = wmspecInfoAction.execute(match['wmtask_id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())

            blockName, dbsBlock = None, None
            if not match['input_id']:
                self.logger.info("Adding Production work")
                wmspecInfo['mask_url'] = None
                lumi_path = os.path.join(self.params['CacheDir'],
                                              "%s.mask" % match['id'])
                if os.path.exists(lumi_path):
                    wmspecInfo['mask_url'] = lumi_path

            if self.params['PopulateFilesets']:
                if not wmspecCache.has_key(wmspecInfo['id']):
                        wmspec = WMWorkloadHelper()
                        # the url should be url from cache 
                        # (check whether that is updated correctly in DB)
                        wmspec.load(wmspecInfo['url'])    
                        wmspecCache[wmspecInfo['id']] = wmspec

                if match['input_id']:
                    self.logger.info("Adding Processing work")
                    blockName, dbsBlock = self._getDBSBlock(match,
                                                wmspecInfo['dbs_url'],
                                                wmspecCache[wmspecInfo['id']])
                else:
                    self.logger.info("Adding Production work")
                    wmspecInfo['mask_url'] = None
                    lumi_path = os.path.join(self.params['CacheDir'],
                                              "%s.mask" % match['id'])
                    if os.path.exists(lumi_path):
                            wmspecInfo['mask_url'] = lumi_path
                status = 'Acquired'
            else:
                status = pullingQueueUrl and 'Negotiating' or 'Acquired'
                # send data to child queue - used to override data in workflow
                if match['input_id'] and pullingQueueUrl:
                    dataLoader = self.daofactory(classname = "Data.LoadByID")
                    data = dataLoader.execute(match['input_id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
                    wmspecInfo['data'] = data['name']
        
            #make one transaction
            with self.transactionContext():
                if self.params['PopulateFilesets']:
                    subscription = self._wmbsPreparation(match, 
                                          wmspecCache[wmspecInfo['id']],
                                          wmspecInfo, blockName, dbsBlock)
                    # subscription object can be added since this call will be
                    # made in local queue (doen't have worry about jsonizing the
                    # result for remote call (over http))
                    wmspecInfo["subscription"] = subscription    
                
                self.setStatus(status, match['id'], 'id', pullingQueueUrl)
                self.logger.info("Updated status for %s '%s'" % 
                                  (match['id'], status))       
                
            wmspecInfo['element_id'] = match['id']
            wmspecInfo['team_name'] = match.get('team_name')
            wmspecInfo['request_name'] = match.get('request_name')
            results.append(wmspecInfo)

        return results
    
    def _getDBSBlock(self, match, dbs, wmspec):
        
        blockLoader = self.daofactory(classname = "Data.LoadByID")
            
        block = blockLoader.execute(match['input_id'],
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
        
        acdcInfo = wmspec.getTopLevelTask()[0].getInputACDC()
        
        
        if acdcInfo:
            acdc = DataCollectionService(acdcInfo["server"], acdcInfo["database"])
            collection = acdc.getDataCollection(acdcInfo['collection'])
            splitedBlockName = ACDCBlock.splitBlockName(block['name'])
            fileLists = acdc.getChunkFiles(collection,
                                           acdcInfo['fileset'],
                                           splitedBlockName['Offset'],
                                           splitedBlockName['NumOfFiles'])
            return block['name'], fileLists
        else:
            #TODO: move this out of the transactions
            dbs = self._get_dbs(dbs)
            if match['parent_flag']:
                dbsBlockDict = dbs.getFileBlockWithParents(block["name"])
            else:
                dbsBlockDict = dbs.getFileBlock(block["name"])
            
            return block['name'], dbsBlockDict[block['name']]


    def _wmbsPreparation(self, match, wmspec, wmspecInfo, blockName, block):
        """
        """
        self.logger.info("Adding WMBS subscription")

        mask = None
        if wmspecInfo.get('mask_url'):
            with open(wmspecInfo['mask_url']) as mask_file:
                mask = pickle.load(mask_file)

        from WMCore.WorkQueue.WMBSHelper import WMBSHelper
        wmbsHelper = WMBSHelper(wmspec, blockName, mask)

        sub = wmbsHelper.createSubscriptionAndAddFiles(block = block)
        self.logger.info("Created top level Subscription %s" % sub['id'])

        updateSub = self.daofactory(classname = "WorkQueueElement.UpdateSubscription")
        updateSub.execute(match['id'], sub['id'],
                                conn = self.getDBConn(),
                                transaction = self.existingTransaction())

        self.logger.info('WMBS subscription (%s) is created for element (%s)' 
                         % (sub['id'], match['id']))
        return sub

    def doneWork(self, elementIDs, id_type = 'id'):
        """
        _doneWork_

        this is called by JSM
        update the WorkQueue status table
        """
        try:
            self.setStatus('Done', elementIDs, id_type)
        except RuntimeError:
            if id_type == "subscription_id":
                self.logger.info("""Done Update: Only some subscription is 
                                    updated Might be the child subscriptions: %s""" 
                                    % elementIDs)
                return elementIDs
            else:
                raise
        return elementIDs

    def failWork(self, elementIDs, id_type = 'id'):
        """Mark work as failed"""
        try:
            self.setStatus('Failed', elementIDs, id_type)
        except RuntimeError:
            if id_type == "subscription_id":
                self.logger.info("""Fail update: Only some subscription is 
                                    updated Might be the child subscriptions: %s""" 
                                    % elementIDs)
                return elementIDs
            else:
                raise
        return elementIDs

    def cancelWork(self, elementIDs, id_type = 'id'):
        """Mark work as canceled
           id_type defines type of elementIDs argument. if it is request_name,
           elementIDs is a list of request names, if it is subscription_id it is the list of
           subscription ids.
        """
        try:
            self.setStatus('Canceled', elementIDs, id_type)
        except RuntimeError:
            if id_type == "subscription_id":
                self.logger.info("""Cancel update: Only some subscription is 
                                    updated.
                                    This might be the child subscriptions: %s"""
                                    % elementIDs)
                return elementIDs
            else:
                raise

        # if it is not local queue,
        if not self.params['LocalQueueFlag'] and id_type == 'request_name':
            # get list of child queue
            qAction = self.daofactory(classname = "WorkQueueElement.ChildQueuesByRequest")
            childQueues = qAction.execute(elementIDs, conn = self.getDBConn(),
                                     transaction = self.existingTransaction())

            for childQueue in childQueues:
                try:
                    childWQ = self._get_remote_queue(childQueue)
                    childWQ.cancelWork(elementIDs, id_type)
                except Exception, ex:
                    # if canceling fails just log the error message.
                    # it will be picked up later when updateParent call occurs
                    # in WorkQueueManager
                    self.logger.warning("canceling work failed on : %s for request %s: %s" % (
                                       childQueue, elementIDs, str(ex)))

        return elementIDs

    def deleteWork(self, elementIDs):
        """
        _deleteWork_

        this is called by JSM
        update the WorkQueue status table
        """
        action = self.daofactory(classname = "WorkQueueElement.Delete")
        action.execute(ids = elementIDs,
                       conn = self.getDBConn(),
                       transaction = self.existingTransaction())

    def deleteFinishedWork(self, elementResults):
        """Delete complete work"""
        complete_results = [x for x in elementResults if x.inEndState()]
        if complete_results:
            complete_ids = []
            msg = 'Finished with task %s of workflow %s as it is %s'
            for result in complete_results:
                complete_ids.extend(x['Id'] for x in result['Elements'])
                an_element = result['Elements'][0]
                self.logger.info(msg % (an_element['Task'],
                                        an_element['WMSpec'].name(),
                                        result['Status']))
            self.deleteWork(complete_ids)

    def queueWork(self, wmspecUrl, parentQueueId = None,
                  team = None, request = None):
        """
        Take and queue work from a WMSpec
        """
        wmspec = WMWorkloadHelper()
        wmspec.load(wmspecUrl)

        totalUnits = self._splitWork(wmspec, parentQueueId, team = team)

        # Do database stuff in one quick loop
        with self.transactionContext():
            dataLocations = {}
            for unit in totalUnits:
                self._insertWorkQueueElement(unit, request, team)
                if unit['Sites'] != None:
                    dataLocations[unit['Data']] = unit['Sites']
                            
            if len(dataLocations) > 0:
                self.dataLocationMapper(dataLocations = dataLocations)

        return len(totalUnits)


    def status(self, status = None, before = None, after = None, elementIDs = None,
               dictKey = None, syncWithWMBS = False, parentId = None):
        """Return status of elements
           Note: optional parameters are AND'ed together
        """
        action = self.daofactory(classname = "WorkQueueElement.GetElements")
        items = action.execute(since = after,
                              before = before,
                              status = status,
                              elementIDs = elementIDs,
                              parentId = parentId,
                              conn = self.getDBConn(),
                              transaction = self.existingTransaction())

        if syncWithWMBS:
            from WMCore.WorkQueue.WMBSHelper import wmbsSubscriptionStatus
            wmbs_status = wmbsSubscriptionStatus(logger = self.logger,
                                                 dbi = self.dbi,
                                                 conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
            for item in items:
                for wmbs in wmbs_status:
                    if item['SubscriptionId'] == wmbs['subscription_id']:
                        item.updateFromSubscription(wmbs)
                        break

        # if dictKey given format as a dict with the appropriate key
        if dictKey:
            tmp = defaultdict(list)
            for item in items:
                tmp[item[dictKey]].append(item)
            items = dict(tmp)
        return items


    def statusReqMgrUpdateNeeded(self, dictKey = None):
        """Return elements that need to update ReqMgr"""
        action = self.daofactory(classname = "WorkQueueElement.GetReqMgrUpdateNeeded")
        items = action.execute(conn = self.getDBConn(),
                               transaction = self.existingTransaction())
        # This can be added inside of the if dictKey check to prevent going
        # through one more loop since that is the only case this function is
        # needed so far, but that is not general case.
        wmspecCache = {}
        for element in items:
            if not wmspecCache.has_key(element['RequestName']):
                wmspec = WMWorkloadHelper()
                wmspec.load(element['WMSpecUrl'])
                wmspecCache[element['RequestName']] = wmspec
            else:
                wmspec = wmspecCache[element['RequestName']]
            element['WMSpec'] = wmspec
        # if dictKey given format as a dict with the appropriate key
        if dictKey:
            tmp = defaultdict(list)
            for item in items:
                tmp[item[dictKey]].append(item)
            items = dict(tmp)
        return items

    def synchronize(self, child_url, child_report):
        """
        Take status from child queue and update ourselves
        """
        my_details = self.status(elementIDs = [x['ParentQueueId'] for x in child_report],
                                 dictKey = "Id")
        #store elements we need to update grouped by status(reduce connections)
        to_update = defaultdict(set)
        # may need to change child status - i.e. if canceled in parent
        child_update = defaultdict(set)

        # when child state is change to canceled, we need mark all wmbs jobs
        # as failed killWorkflow(workflow)
        # elements who need their progress updated
        progress_updates = []

        self.logger.info("synchronize queue with child %s" % child_url)
        self.logger.debug("report contents: %s" % str(child_report))
        self.logger.debug("current state: %s" % str(my_details))

        for item in child_report:
            item_id = item['ParentQueueId']

            # This queue doesn't know about the work - ignore
            if not my_details.has_key(item_id):
                continue

            my_item = my_details[item_id]
            assert(len(my_item) == 1)
            my_item = my_item[0]

            # New in child equates to Acquired in parent
            if item['Status'] == 'Available':
                item['Status'] = 'Acquired'

            # Negotiation failure - Another queue has the work
            if my_item['ChildQueueUrl'] != child_url:
                msg = "Negotiation failure for element %s now assigned to %s"
                self.logger.warning(msg % (item_id, my_item['ChildQueueUrl']))
                child_update['Canceled'].add(my_item['Id'])
                continue

            if my_item.progressUpdate(item):
                progress_updates.append(item)

            # if status's the same no need to update anything
            if item['Status'] == my_item['Status']:
                continue
            # From here on either this queue or the child needs to be updated

            # if parent in final state (manual intervention?) force child to same state
            if my_item['Status'] in ('Done', 'Failed', 'Canceled'):
                # force child to same state
                child_update[my_item['Status']].add(my_item['Id'])
                continue

            to_update[item['Status']].add(my_item['Id'])

        self.logger.debug('Synchronise() updates: %s' % str(to_update))
        with self.transactionContext():
            for ele in progress_updates:
                self.setProgress(ele, id = 'ParentQueueId')

            for status, items in to_update.items():
                self.setStatus(status, items, source = child_url)

        # return to the child queue the elements that it needs to update
        self.logger.debug('Updates to child queue: %s' % str(child_update))
        return dict(child_update) # Service json lib can't handle defaultdict


    def flushNegotiationFailures(self):
        """
        Check for any elements that have been Negotiating for too long,
        and reset them to allow them to be acquired again.
        """
        items = self.daofactory(classname = "WorkQueueElement.GetExpiredElements")
        items = items.execute(conn = self.getDBConn(),
                              status = 'Negotiating',
                              interval = self.params['NegotiationTimeout'],
                              transaction = self.existingTransaction())
        if items:
            # log negotiation failures and setStatus to available
            self.logger.info("Reset expired negotiations: %s" % str(items))
            self.setStatus('Available', [x['id'] for x in items])
        return len(items)






    #  //
    # // Methods that call out to remote services
    #//

    def updateLocationInfo(self, newDataOnly = False):
        """
        Update locations for elements
        """
        return self.dataLocationMapper(newDataOnly, dbses = self.dbsHelpers)

    def pullWork(self, resources = None):
        """
        Pull work from another WorkQueue to be processed

        If resources passed in get work for them, if not get from wmbs.
        """
        wmspecCache = {}
        counter = 0
        if self.parent_queue:
            for team in self.params['Teams']:
                self.logger.info("Getting Work for Team: %s" % team)
                totalUnits = []
                if not resources:
                    from WMCore.ResourceControl.ResourceControl import ResourceControl
                    rc_sites = ResourceControl().listThresholdsForCreate()
                    # get more work than we have slots - QueueDepth param
                    sites = {}
                    [sites.__setitem__(name,
                        self.params['QueueDepth'] * slots['total_slots'])
                        for name, slots in rc_sites.items() if slots['total_slots'] > 0]
                    self.logger.info("""Pull work for sites %s 
                                        with %s queue depth
                                     """ % (str(sites), 
                                            self.params['QueueDepth']))
                    _, resources = self._match(sites)

                # if we have sites with no queued work try and get some
                if resources:
                    work = self.parent_queue.getWork(resources,
                                                     self.params['QueueURL'],
                                                     team)
                    if work:
                        for element in work:
                            # to prevent creating multiple instance of the same
                            # spec, use the cache (cache with in the function)
                            # id here is a unique spec id
                            if not wmspecCache.has_key(element['id']):
                                wmspec = WMWorkloadHelper()
                                wmspec.load(element['url'])
                                wmspecCache[element['id']] = wmspec
                            else:
                                wmspec = wmspecCache[element['id']]
                            # check we haven't seen this before
                            if self.params['IgnoreDuplicates'] and self.status(parentId = element['element_id']):
                                self.logger.warning('Ignoring duplicate work: %s' % wmspec.name())
                                continue
                            
                            mask = None
                            if element.get('mask_url'):
                                maskLoader = PersistencyHelper()
                                maskLoader.load(element['mask_url'])
                                mask = maskLoader.data
                            totalUnits.extend(self._splitWork(wmspec,
                                                        element['element_id'],
                                                        element.get('data'),
                                                        mask,
                                                        element['team_name']))
                            self.logger.info("Getting element form parent queue: %s" % element.get('data'))

                        dataLocations = {}
                        with self.transactionContext():
                            for unit in totalUnits:
                                # spec name and request name should be always identical.
                                self._insertWorkQueueElement(unit,
                                                             unit['RequestName'],
                                                             unit['TeamName'])
                                if unit['Sites'] != None:
                                    dataLocations[unit['Data']] = unit['Sites']
                            
                            if len(dataLocations) > 0:
                                self.logger.info("acdc block location %s" % dataLocations)
                                self.dataLocationMapper(dataLocations = dataLocations)
                                
                        counter += len(totalUnits)
        return counter

    def updateParent(self, full = False, skipWMBS = False):
        """
        Report status of elements to the parent queue

        By default only report elements that have changed since the last update
        """
        if self.parent_queue is None:
            return

        # check whether we need to do a full report
        now = time.time()
        if not full:
            full = self.lastFullReportToParent + \
                   self.params['FullReportInterval'] < now
        if full:
            since = None
        else:
            since = self.lastReportToParent

        # Get queue elements grouped by their parent with updated wmbs progress
        useWMBS = not skipWMBS and self.params['LocalQueueFlag']
        elements = self.status(after = since, dictKey = "ParentQueueId",
                               syncWithWMBS = useWMBS)
        # filter elements that don't come from the parent
        elements.pop(None, None)
        # apply end policy to elements grouped by parent
        wmspecCache = {}
        results = []
        for group in elements.values():
            for ele in group:
                if not wmspecCache.has_key(ele['RequestName']):
                    wmspec = WMWorkloadHelper()
                    # the url should be url from cache
                    # (check whether that is updated correctly in DB)
                    wmspec.load(ele['WMSpecUrl'])
                    wmspecCache[ele['RequestName']] = wmspec
                ele.__setitem__('WMSpec', wmspecCache[ele['RequestName']])

            results.append(endPolicy(group, self.params['EndPolicySettings']))

        # Need to be in dict format for sending over the wire
        items = [x.formatForWire() for x in results]

        if items:
            self.logger.debug("Update parent queue with: %s" % str(items))
            try:
                # send to remote queue
                # check that we don't have an error from incompatible states
                # i.e. canceled in parent - if so cancel here...
                result = self.parent_queue.synchronize(self.params['QueueURL'],
                                                       items)
            except Exception, ex:
                # log a failure to communicate
                msg = "Unable to send update to parent queue, error: %s"
                self.logger.warning(msg % str(ex))
                result = {}
            else:
                # some of our element status's may be overriden by the parent
                # e.g. if request is canceled at top level
                # also, save new wmbs status
                wmbs_updated = []
                for i in elements.values():
                    wmbs_updated.extend([x for x in i if x['Modified']])
                if result or wmbs_updated:
                    with self.transactionContext():

                        # first update status from wmbs
                        for ele in wmbs_updated:
                            self.setProgress(ele)

                        msg = "Parent queue status override to %s for %s"
                        for status, ids in result.items():
                            self.logger.debug(msg % (status, list(ids)))
                            self.setStatus(status, ids, id_type = 'parent_queue_id')

                # prune elements that are finished (after reporting to parent)
                self.deleteFinishedWork(results)

        # record update times
        if full:
            self.lastFullReportToParent = now
        else:
            self.lastReportToParent = now



    #  //
    # //  Internal methods
    #//

    def _splitWork(self, wmspec, parentQueueId = None,
                   data = None, mask = None, team = None):
        """
        Split work into WorkQeueueElements

        If data param supplied use that rather than getting input data from
        wmspec. Used for instance when global splits by Block (avoids having to
        modify wmspec block whitelist - thus all appear as same wf in wmbs)
        """
        #TODO: This can leave orphan files behind - remove them on exception
        totalUnits = []

        # split each top level task into constituent work elements
        # get the acdc server and db name
        for topLevelTask in wmspec.taskIterator():
            dbs_url = topLevelTask.dbsUrl()

            try:
                if dbs_url:
                    self._get_dbs(dbs_url)

                policyName = wmspec.startPolicy()
                if not policyName:
                    raise RuntimeError("WMSpec doens't define policyName, current value: '%s'" % policyName)

                # update policy parameter
                self.params['SplittingMapping'][policyName].update(args = wmspec.startPolicyParameters())
                policy = startPolicy(policyName, self.params['SplittingMapping'])
                self.logger.info("Using %s start policy with %s " % (policyName,
                                                self.params['SplittingMapping']))
                units = policy(wmspec, topLevelTask, self.dbsHelpers, data, mask, team)
                for unit in units:
                    unit['ParentQueueId'] = parentQueueId
                self.logger.info("Queuing %s unit(s): wf: %s for task: %s" % (
                                 len(units), wmspec.name(), topLevelTask.name()))
                totalUnits.extend(units)
            # some dbs errors should be considered fatal
            except (DbsConfigurationError), ex:
                error = WorkQueueWMSpecError(wmspec, "DBS config error: %s" % str(ex))
                raise error

        return totalUnits

    def _insertWorkQueueElement(self, unit, requestName = None,
                                teamName = None):
        """
        Persist a block to the database
        """
        primaryInput = unit['Data']
        parentInputs = unit['ParentData']
        nJobs = unit['Jobs']
        wmspec = unit['WMSpec']
        task = unit["Task"]
        parentQueueId = unit['ParentQueueId']
        # if requestName is not specified get the name from wmspec
        if requestName == None:
            requestName = wmspec.name()
        if wmspec.name() != requestName:
            error = WorkQueueWMSpecError(wmspec, 
                      "WMSpec Name error: %s doesn't match with request name %s" %
                      (wmspec.name(), requestName))
            raise error
        
        self._insertWMSpec(wmspec)
        self._insertWMTask(wmspec.name(), task)

        if primaryInput:
            self._insertInputs(primaryInput, parentInputs)

        wqAction = self.daofactory(classname = "WorkQueueElement.New")
        parentFlag = parentInputs and 1 or 0
        priority = wmspec.priority() or 1

        elementID = wqAction.execute(wmspec.name(), task.name(), primaryInput,
                         nJobs, priority, parentFlag, parentQueueId,
                         requestName, teamName,
                         conn = self.getDBConn(),
                         transaction = self.existingTransaction())

        whitelist = task.siteWhitelist()
        if whitelist:
            self._insertWhiteList(elementID, whitelist)
        blacklist = task.siteBlacklist()
        if blacklist:
            self._insertBlackList(elementID, blacklist)

        if unit.get('Mask'):
            import pickle
            with open(os.path.join(self.params['CacheDir'],
                                   "%s.mask" % elementID), 'wb') as mask_file:
                pickle.dump(unit['Mask'], mask_file)

        return elementID

    def _insertWMSpec(self, wmspec):
        """
        """
        existsAction = self.daofactory(classname = "WMSpec.Exists")
        exists = existsAction.execute(wmspec.name(), conn = self.getDBConn(),
                             transaction = self.existingTransaction())

        if not exists:
            
            if self.params['LocalQueueFlag']:
                from WMCore.WMRuntime.SandboxCreator import SandboxCreator
                sandboxCreator = SandboxCreator()
                sandboxCreator.makeSandbox(self.params['CacheDir'], wmspec)
            else:
                #TODO: This might not be needed if the getWorkReturns json of wmspec
                #Also if we need local cache need to clean up sometime
                localCache = os.path.join(self.params['CacheDir'], "%s.pkl" % wmspec.name())
                wmspec.setSpecUrl(localCache)
                wmspec.save(localCache)

            wmspecAction = self.daofactory(classname = "WMSpec.New")
            owner = "/".join([str(x) for x in wmspec.getOwner().values()])
            wmspecAction.execute(wmspec.name(), wmspec.specUrl(), owner,
                                 conn = self.getDBConn(),
                                 transaction = self.existingTransaction())
        
        return exists

    def _insertWMTask(self, wmspecName, task):
        """
        """
        existsAction = self.daofactory(classname = "WMSpec.ExistsTask")
        exists = existsAction.execute(wmspecName, task.name(), 
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

        if not exists:            
            taskAction = self.daofactory(classname = "WMSpec.AddTask")
            taskAction.execute(wmspecName, task.name(), task.dbsUrl(), task.taskType(),
                               conn = self.getDBConn(),
                               transaction = self.existingTransaction())
        return exists

    def _insertWhiteList(self, elementID, whitelist):
        """
        """
        self._insertSite(whitelist)

        whitelistAction = self.daofactory(classname = "Site.AddWhiteList")
        whitelistAction.execute(elementID, whitelist, conn = self.getDBConn(),
                             transaction = self.existingTransaction())

    def _insertBlackList(self, elementID, blacklist):
        """
        """
        self._insertSite(blacklist)
        blacklistAction = self.daofactory(classname = "Site.AddBlackList")
        blacklistAction.execute(elementID, blacklist, conn = self.getDBConn(),
                             transaction = self.existingTransaction())


    def _insertInputs(self, primary, parents):
        """
        Insert blocks and record parentage info
        """
        def _inputCreation(data):
            """
            Internal function to insert an input
            """
            exists = existsAction.execute(data, 
                                    conn = self.getDBConn(),
                                    transaction = self.existingTransaction())
            if not exists:
                dataAction.execute(data, conn = self.getDBConn(),
                               transaction = self.existingTransaction())
        
        
        existsAction = self.daofactory(classname = "Data.Exists")
        dataAction = self.daofactory(classname = "Data.New")
        dataParentageAct = self.daofactory(classname = "Data.AddParent")

        _inputCreation(primary)
        for parent in parents:
            _inputCreation(parent)
            dataParentageAct.execute(primary,
                                      parent,
                                      conn = self.getDBConn(),
                                      transaction = self.existingTransaction())

    def _insertSite(self, sites):
        """
        Insert site into database
        """
        siteAction = self.daofactory(classname = "Site.New")
        siteAction.execute(sites,
                           conn = self.getDBConn(),
                           transaction = self.existingTransaction())

    def _match(self, conditions, team = None):
        """
        Match resources to available work
        """
        matchAction = self.daofactory(classname = "WorkQueueElement.GetWork")
        elements, unmatched = matchAction.execute(conditions, team,
                                       self.params['ItemWeight'],
                                       conn = self.getDBConn(),
                                       transaction = self.existingTransaction())
        return elements, unmatched

    def _get_remote_queue(self, queue):
        """
        Get an object to talk to a remote queue
        """
        # tests generally get the queue object passed in direct
        if isinstance(queue, WorkQueue):
            return queue
        try:
            return self.remote_queues[queue]
        except KeyError:
            self.remote_queues[queue] = WorkQueueDS({'endpoint' : queue,
                                                     'logger' : self.logger})
            return self.remote_queues[queue]

    def _get_dbs(self, url):
        """Return DBS object for url"""
        if url is None: # fall back to global
            url = self.params['GlobalDBS']
        try:
            return self.dbsHelpers[url]
        except KeyError:
            self.dbsHelpers[url] = DBSReader(url)
            return self.dbsHelpers[url]
