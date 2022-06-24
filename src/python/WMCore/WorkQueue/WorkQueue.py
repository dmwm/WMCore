#!/usr/bin/env python

"""
WorkQueue provides functionality to queue large chunks of work,
thus acting as a buffer for the next steps in job processing

WMSpec objects are fed into the queue, split into coarse grained work units
and released when a suitable resource is found to execute them.

https://twiki.cern.ch/twiki/bin/view/CMS/WMCoreJobPool
"""

from __future__ import division, print_function

from builtins import str as newstr, bytes
from future.utils import viewitems, listvalues

import os
import threading
import time
from collections import defaultdict

from WMCore import Lexicon
from WMCore.ACDC.DataCollectionService import DataCollectionService
from WMCore.Database.CMSCouch import CouchInternalServerError, CouchNotFoundError
from WMCore.Services.CRIC.CRIC import CRIC
from WMCore.Services.DBS.DBSReader import DBSReader
from WMCore.Services.LogDB.LogDB import LogDB
from WMCore.Services.ReqMgr.ReqMgr import ReqMgr
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.Services.Rucio.Rucio import Rucio
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue as WorkQueueDS
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper, getWorkloadFromTask
from WMCore.WorkQueue.DataLocationMapper import WorkQueueDataLocationMapper
from WMCore.WorkQueue.DataStructs.ACDCBlock import ACDCBlock
from WMCore.WorkQueue.DataStructs.WorkQueueElement import possibleSites
from WMCore.WorkQueue.DataStructs.WorkQueueElementsSummary import getGlobalSiteStatusSummary
from WMCore.WorkQueue.Policy.End import endPolicy
from WMCore.WorkQueue.Policy.Start import startPolicy
from WMCore.WorkQueue.WorkQueueBackend import WorkQueueBackend
from WMCore.WorkQueue.WorkQueueBase import WorkQueueBase
from WMCore.WorkQueue.WorkQueueExceptions import (TERMINAL_EXCEPTIONS, WorkQueueError, WorkQueueNoMatchingElements,
                                                  WorkQueueWMSpecError)
from WMCore.WorkQueue.WorkQueueUtils import cmsSiteNames


# Convenience constructor functions

def globalQueue(logger=None, dbi=None, **kwargs):
    """Convenience method to create a WorkQueue suitable for use globally
    """
    defaults = {'PopulateFilesets': False,
                'LocalQueueFlag': False,
                'TrackLocationOrSubscription': 'location'
                }
    defaults.update(kwargs)
    return WorkQueue(logger, dbi, **defaults)


def localQueue(logger=None, dbi=None, **kwargs):
    """Convenience method to create a WorkQueue suitable for use locally
    """
    defaults = {'TrackLocationOrSubscription': 'location'}
    defaults.update(kwargs)
    return WorkQueue(logger, dbi, **defaults)


class WorkQueue(WorkQueueBase):
    """
    _WorkQueue_

    WorkQueue object - interface to WorkQueue functionality.
    """

    def __init__(self, logger=None, dbi=None, **params):

        WorkQueueBase.__init__(self, logger, dbi)
        self.parent_queue = None
        self.params = params

        # config argument (within params) shall be reference to
        # Configuration instance
        self.config = params.get("Config", None)
        self.params.setdefault('CouchUrl', os.environ.get('COUCHURL'))
        if not self.params.get('CouchUrl'):
            raise RuntimeError('CouchUrl config value mandatory')
        self.params.setdefault('DbName', 'workqueue')
        self.params.setdefault('InboxDbName', self.params['DbName'] + '_inbox')
        self.params.setdefault('ParentQueueCouchUrl', None)  # We get work from here

        self.backend = WorkQueueBackend(self.params['CouchUrl'], self.params['DbName'],
                                        self.params['InboxDbName'],
                                        self.params['ParentQueueCouchUrl'], self.params.get('QueueURL'),
                                        logger=self.logger)
        self.workqueueDS = WorkQueueDS(self.params['CouchUrl'], self.params['DbName'],
                                       self.params['InboxDbName'])
        if self.params.get('ParentQueueCouchUrl'):
            try:
                if self.params.get('ParentQueueInboxCouchDBName'):
                    self.parent_queue = WorkQueueBackend(self.params['ParentQueueCouchUrl'].rsplit('/', 1)[0],
                                                         self.params['ParentQueueCouchUrl'].rsplit('/', 1)[1],
                                                         self.params['ParentQueueInboxCouchDBName'])
                else:
                    self.parent_queue = WorkQueueBackend(self.params['ParentQueueCouchUrl'].rsplit('/', 1)[0],
                                                         self.params['ParentQueueCouchUrl'].rsplit('/', 1)[1])
            except IndexError as ex:
                # Probable cause: Someone didn't put the global WorkQueue name in
                # the ParentCouchUrl
                msg = "Parsing failure for ParentQueueCouchUrl - probably missing dbname in input\n"
                msg += "Exception: %s\n" % str(ex)
                msg += str("ParentQueueCouchUrl: %s\n" % self.params['ParentQueueCouchUrl'])
                self.logger.error(msg)
                raise WorkQueueError(msg)
            self.params['ParentQueueCouchUrl'] = self.parent_queue.queueUrl

        # save each DBSReader instance in the class object, such that
        # the same object is not shared amongst multiple threads
        self.dbses = {}

        self.params.setdefault('QueueDepth', 1)  # when less than this locally
        self.params.setdefault('WorkPerCycle', 100)
        self.params.setdefault('LocationRefreshInterval', 600)
        self.params.setdefault('FullLocationRefreshInterval', 7200)
        self.params.setdefault('TrackLocationOrSubscription', 'location')
        self.params.setdefault('ReleaseIncompleteBlocks', False)
        self.params.setdefault('ReleaseRequireSubscribed', True)
        self.params.setdefault('PopulateFilesets', True)
        self.params.setdefault('LocalQueueFlag', True)
        self.params.setdefault('QueueRetryTime', 86400)
        self.params.setdefault('stuckElementAlertTime', 172800)
        self.params.setdefault('reqmgrCompleteGraceTime', 604800)
        self.params.setdefault('cancelGraceTime', 86400)

        self.params.setdefault('JobDumpConfig', None)
        self.params.setdefault('BossAirConfig', None)

        self.params['QueueURL'] = self.backend.queueUrl  # url this queue is visible on
        # backend took previous QueueURL and sanitized it
        self.params.setdefault('WMBSUrl', None)  # this will only be set on local Queue
        if self.params.get('WMBSUrl'):
            self.params['WMBSUrl'] = Lexicon.sanitizeURL(self.params['WMBSUrl'])['url']
        self.params.setdefault('Team', "")

        if self.params.get('CacheDir'):
            try:
                os.makedirs(self.params['CacheDir'])
            except OSError:
                pass
        elif self.params.get('PopulateFilesets'):
            raise RuntimeError('CacheDir mandatory for local queue')

        if self.params.get('CRIC'):
            self.cric = self.params['CRIC']
        else:
            self.cric = CRIC()

        self.params.setdefault('SplittingMapping', {})
        self.params['SplittingMapping'].setdefault('DatasetBlock',
                                                   {'name': 'Block',
                                                    'args': {}}
                                                   )
        self.params['SplittingMapping'].setdefault('MonteCarlo',
                                                   {'name': 'MonteCarlo',
                                                    'args': {}}
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

        assert (self.params['TrackLocationOrSubscription'] in ('subscription',
                                                               'location'))
        # Can only release blocks on location
        if self.params['TrackLocationOrSubscription'] == 'location':
            if self.params['SplittingMapping']['DatasetBlock']['name'] != 'Block':
                raise RuntimeError('Only blocks can be released on location')

        self.params.setdefault('rucioAccount', "wmcore_transferor")

        self.rucio = Rucio(self.params['rucioAccount'],
                           self.params['rucioUrl'], self.params['rucioAuthUrl'],
                           configDict=dict(logger=self.logger))


        self.dataLocationMapper = WorkQueueDataLocationMapper(self.logger, self.backend,
                                                              rucio=self.rucio,
                                                              cric=self.cric,
                                                              locationFrom=self.params['TrackLocationOrSubscription'],
                                                              incompleteBlocks=self.params['ReleaseIncompleteBlocks'],
                                                              requireBlocksSubscribed=not self.params[
                                                                  'ReleaseIncompleteBlocks'],
                                                              fullRefreshInterval=self.params[
                                                                  'FullLocationRefreshInterval'],
                                                              updateIntervalCoarseness=self.params[
                                                                  'LocationRefreshInterval'])

        # used for only global WQ
        if self.params.get('ReqMgrServiceURL'):
            self.reqmgrSvc = ReqMgr(self.params['ReqMgrServiceURL'])

        if self.params.get('RequestDBURL'):
            # This is need for getting post call
            # TODO: Change ReqMgr api to accept post for for retrieving the data and remove this
            self.requestDB = RequestDBReader(self.params['RequestDBURL'])

        # set the thread name before create the log db.
        # only sets that when it is not set already
        # setLogDB

        myThread = threading.currentThread()
        if myThread.getName() == "MainThread":  # this should be only GQ case other cases thread name should be set
            myThread.setName(self.__class__.__name__)

        centralurl = self.params.get("central_logdb_url")
        identifier = self.params.get("log_reporter")
        self.logdb = LogDB(centralurl, identifier, logger=self.logger)

        self.logger.debug("WorkQueue created successfully")

    def __len__(self):
        """Returns number of Available elements in queue"""
        return self.backend.queueLength()

    def setStatus(self, status, elementIDs=None, SubscriptionId=None, WorkflowName=None):
        """
        _setStatus_, throws an exception if no elements are updated

        """
        try:
            if not elementIDs:
                elementIDs = []
            iter(elementIDs)
            if isinstance(elementIDs, (newstr, bytes)):
                raise TypeError
        except TypeError:
            elementIDs = [elementIDs]

        if status == 'Canceled':  # Cancel needs special actions
            return self.cancelWork(elementIDs, SubscriptionId, WorkflowName)

        args = {}
        if SubscriptionId:
            args['SubscriptionId'] = SubscriptionId
        if WorkflowName:
            args['RequestName'] = WorkflowName

        affected = self.backend.getElements(elementIDs=elementIDs, **args)
        if not affected:
            raise WorkQueueNoMatchingElements("No matching elements")

        for x in affected:
            x['Status'] = status
        elements = self.backend.saveElements(*affected)
        if len(affected) != len(elements):
            raise RuntimeError("Some elements not updated, see log for details")

        return elements

    def setPriority(self, newpriority, *workflowNames):
        """
        Update priority for a workflow, throw exception if no elements affected
        """
        self.logger.info("Priority change request to %s for %s", newpriority, str(workflowNames))
        affected = []
        for wf in workflowNames:
            affected.extend(self.backend.getElements(returnIdOnly=True, RequestName=wf))

        self.backend.updateElements(*affected, Priority=newpriority)

        if not affected:
            raise RuntimeError("Priority not changed: No matching elements")

    def resetWork(self, ids):
        """Put work back in Available state, from here either another queue
         or wmbs can pick it up.

         If work was Acquired by a child queue, the next status update will
         cancel the work in the child.

         Note: That the same child queue is free to pick the work up again,
          there is no permanent blacklist of queues.
        """
        self.logger.info("Resetting elements %s", str(ids))
        try:
            iter(ids)
        except TypeError:
            ids = [ids]

        return self.backend.updateElements(*ids, Status='Available',
                                           ChildQueueUrl=None, WMBSUrl=None)

    def getWork(self, jobSlots, siteJobCounts, excludeWorkflows=None):
        """
        Get available work from the queue, inject into wmbs & mark as running

        jobSlots is dict format of {site: estimateJobSlot}
        of the resources to get work for.

        siteJobCounts is a dict format of {site: {prio: jobs}}
        """
        excludeWorkflows = excludeWorkflows or []
        results = []
        numElems = self.params['WorkPerCycle']
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: skipping fetching of work')
            return results

        matches, _ = self.backend.availableWork(jobSlots, siteJobCounts,
                                                excludeWorkflows=excludeWorkflows, numElems=numElems)

        self.logger.info('Got %i elements matching the constraints', len(matches))
        if not matches:
            return results

        myThread = threading.currentThread()
        # cache wmspecs for lifetime of function call, likely we will have multiple elements for same spec.
        # TODO: Check to see if we can skip spec loading - need to persist some more details to element
        wmspecCache = {}
        for match in matches:
            blockName, dbsBlock = None, None
            if self.params['PopulateFilesets']:
                if match['RequestName'] not in wmspecCache:
                    wmspec = self.backend.getWMSpec(match['RequestName'])
                    wmspecCache[match['RequestName']] = wmspec
                else:
                    wmspec = wmspecCache[match['RequestName']]

                try:
                    if match['StartPolicy'] == 'Dataset':
                        # actually returns dataset name and dataset info
                        blockName, dbsBlock = self._getDBSDataset(match)
                    elif match['Inputs']:
                        blockName, dbsBlock = self._getDBSBlock(match, wmspec)
                except Exception as ex:
                    msg = "%s, %s: \n" % (wmspec.name(), list(match['Inputs']))
                    msg += "failed to retrieve data from DBS/Rucio in LQ: \n%s" % str(ex)
                    self.logger.error(msg)
                    self.logdb.post(wmspec.name(), msg, 'error')
                    continue

                try:
                    match['Subscription'] = self._wmbsPreparation(match,
                                                                  wmspec,
                                                                  blockName,
                                                                  dbsBlock)
                    self.logdb.delete(wmspec.name(), "error", this_thread=True)
                except Exception as ex:
                    if getattr(myThread, 'transaction', None) is not None:
                        myThread.transaction.rollback()
                    msg = "Failed to create subscription for %s with block name %s" % (wmspec.name(), blockName)
                    msg += "\nError: %s" % str(ex)
                    self.logger.exception(msg)
                    self.logdb.post(wmspec.name(), msg, 'error')
                    continue

            results.append(match)

        del wmspecCache  # remove cache explicitly
        self.logger.info('Injected %s out of %s units into WMBS', len(results), len(matches))
        return results

    def _getDbs(self, dbsUrl):
        """
        If we have already construct a DBSReader object pointing to
        the DBS URL provided, return it. Otherwise, create and return
        a new instance.
        :param dbsUrl: string with the DBS url
        :return: an instance of DBSReader
        """
        if dbsUrl in self.dbses:
            return self.dbses[dbsUrl]
        return DBSReader(dbsUrl)

    def _getDBSDataset(self, match):
        """Get DBS info for this dataset"""
        tmpDsetDict = {}
        dbs = self._getDbs(match['Dbs'])
        datasetName = list(match['Inputs'])[0]

        blocks = dbs.listFileBlocks(datasetName)
        for blockName in blocks:
            blockSummary = dbs.getFileBlock(blockName)
            blockSummary['PhEDExNodeNames'] = self.rucio.getDataLockedAndAvailable(name=blockName,
                                                                                   account=self.params['rucioAccount'])
            tmpDsetDict[blockName] = blockSummary

        dbsDatasetDict = {'Files': [], 'PhEDExNodeNames': []}
        dbsDatasetDict['Files'] = [f for block in listvalues(tmpDsetDict) for f in block['Files']]
        dbsDatasetDict['PhEDExNodeNames'].extend(
                [f for block in listvalues(tmpDsetDict) for f in block['PhEDExNodeNames']])
        dbsDatasetDict['PhEDExNodeNames'] = list(set(dbsDatasetDict['PhEDExNodeNames']))

        return datasetName, dbsDatasetDict

    def _getDBSBlock(self, match, wmspec):
        """Get DBS info for this block"""
        blockName = list(match['Inputs'])[0]  # TODO: Allow more than one

        if match['ACDC']:
            acdcInfo = match['ACDC']
            acdc = DataCollectionService(acdcInfo["server"], acdcInfo["database"])
            splitedBlockName = ACDCBlock.splitBlockName(blockName)
            fileLists = acdc.getChunkFiles(acdcInfo['collection'],
                                           acdcInfo['fileset'],
                                           splitedBlockName['Offset'],
                                           splitedBlockName['NumOfFiles'])

            block = {}
            block["Files"] = fileLists
            return blockName, block
        else:
            dbs = self._getDbs(match['Dbs'])
            if wmspec.getTask(match['TaskName']).parentProcessingFlag():
                dbsBlockDict = dbs.getFileBlockWithParents(blockName)
                dbsBlockDict['PhEDExNodeNames'] = self.rucio.getDataLockedAndAvailable(name=blockName,
                                                                                       account=self.params['rucioAccount'])
            elif wmspec.getRequestType() == 'StoreResults':
                dbsBlockDict = dbs.getFileBlock(blockName)
                dbsBlockDict['PhEDExNodeNames'] = dbs.listFileBlockLocation(blockName)
            else:
                dbsBlockDict = dbs.getFileBlock(blockName)
                dbsBlockDict['PhEDExNodeNames'] = self.rucio.getDataLockedAndAvailable(name=blockName,
                                                                                       account=self.params['rucioAccount'])

        return blockName, dbsBlockDict

    def _wmbsPreparation(self, match, wmspec, blockName, dbsBlock):
        """Inject data into wmbs and create subscription. """
        from WMCore.WorkQueue.WMBSHelper import WMBSHelper
        # the parent element (from local couch) can be fetch via:
        # curl -ks -X GET 'http://localhost:5984/workqueue/<ParentQueueId>'

        # Keep in mind that WQE contains sites, wmbs location contains pnns
        commonSites = possibleSites(match)
        commonLocation = self.cric.PSNstoPNNs(commonSites, allowPNNLess=True)
        msg = "Running WMBS preparation for %s with ParentQueueId %s,\n  with common location %s"
        self.logger.info(msg, match['RequestName'], match['ParentQueueId'], commonLocation)

        mask = match['Mask']
        wmbsHelper = WMBSHelper(wmspec, match['TaskName'], blockName, mask,
                                self.params['CacheDir'], commonLocation)

        sub, match['NumOfFilesAdded'] = wmbsHelper.createSubscriptionAndAddFiles(block=dbsBlock)
        self.logger.info("Created top level subscription %s for %s with %s files",
                         sub['id'], match['RequestName'], match['NumOfFilesAdded'])

        # update couch with wmbs subscription info
        match['SubscriptionId'] = sub['id']
        match['Status'] = 'Running'
        # do update rather than save to avoid conflicts from other thread writes
        self.backend.updateElements(match.id, Status='Running', SubscriptionId=sub['id'],
                                    NumOfFilesAdded=match['NumOfFilesAdded'])
        self.logger.info("LQE %s set to 'Running' for request %s", match.id, match['RequestName'])

        return sub

    def _assignToChildQueue(self, queue, *elements):
        """Assign work from parent to queue"""
        workByRequest = {}
        for ele in elements:
            ele['Status'] = 'Negotiating'
            ele['ChildQueueUrl'] = queue
            ele['ParentQueueUrl'] = self.params['ParentQueueCouchUrl']
            ele['WMBSUrl'] = self.params["WMBSUrl"]
            workByRequest.setdefault(ele['RequestName'], 0)
            workByRequest[ele['RequestName']] += 1
        work = self.parent_queue.saveElements(*elements)
        self.logger.info("Assigned work to the child queue for:")
        for reqName, numElem in viewitems(workByRequest):
            self.logger.info("    %d elements for: %s", numElem, reqName)
        return work

    def doneWork(self, elementIDs=None, SubscriptionId=None, WorkflowName=None):
        """Mark work as done
        """
        return self.setStatus('Done', elementIDs=elementIDs,
                              SubscriptionId=SubscriptionId,
                              WorkflowName=WorkflowName)

    def killWMBSWorkflows(self, reqNames):
        """
        Kill/cancel workflows in WMBS and CouchDB.
        Also update job state transition in three data sources: local couch,
        local WMBS and dashboard.
        :param reqNames: list of request names
        :return: a list of workflows that failed to be cancelled
        """
        failedWfs = []
        if not reqNames:
            return failedWfs

        # import inside function since GQ doesn't need this.
        from WMCore.WorkQueue.WMBSHelper import killWorkflow
        myThread = threading.currentThread()
        myThread.dbi = self.conn.dbi
        myThread.logger = self.logger

        for workflow in reqNames:
            try:
                self.logger.info("Killing workflow in WMBS: %s", workflow)
                killWorkflow(workflow, self.params["JobDumpConfig"], self.params["BossAirConfig"])
            except Exception as ex:
                failedWfs.append(workflow)
                msg = "Failed to kill workflow '%s' in WMBS. Error: %s" % (workflow, str(ex))
                msg += "\nIt will be retried in the next loop"
                self.logger.error(msg)
        return failedWfs

    def cancelWork(self, elementIDs=None, SubscriptionId=None, WorkflowName=None, elements=None):
        """Cancel work - delete in wmbs, delete from workqueue db, set canceled in inbox
           Elements may be directly provided or determined from series of filter arguments
        """
        if not elements:
            args = {}
            if SubscriptionId:
                args['SubscriptionId'] = SubscriptionId
            if WorkflowName:
                args['RequestName'] = WorkflowName
            elements = self.backend.getElements(elementIDs=elementIDs, **args)

        # take wf from args in case no elements exist for workflow (i.e. work was negotiating)
        requestNames = set([x['RequestName'] for x in elements]) | set([wf for wf in [WorkflowName] if wf])
        if not requestNames:
            return []
        inbox_elements = []
        for wf in requestNames:
            inbox_elements.extend(self.backend.getInboxElements(WorkflowName=wf))

        # if local queue, kill jobs, update parent to Canceled and delete elements
        if self.params['LocalQueueFlag']:
            # if we can talk to wmbs kill the jobs
            badWfsCancel = []
            if self.params['PopulateFilesets']:
                self.logger.info("Canceling work for workflow(s): %s", requestNames)
                badWfsCancel = self.killWMBSWorkflows(requestNames)
            # now we remove any wf that failed to be cancelled (and its inbox elements)
            requestNames -= set(badWfsCancel)
            for wf in badWfsCancel:
                elementsToRemove = self.backend.getInboxElements(WorkflowName=wf)
                inbox_elements = list(set(inbox_elements) - set(elementsToRemove))
            self.logger.info("New list of cancelled requests: %s", requestNames)

            # Don't update as fails sometimes due to conflicts (#3856)
            for x in inbox_elements:
                if x['Status'] != 'Canceled':
                    x.load().__setitem__('Status', 'Canceled')

            self.backend.saveElements(*inbox_elements)

        # if global queue, update non-acquired to Canceled, update parent to CancelRequested
        else:
            # Cancel in global if work has not been passed to a child queue
            elements_to_cancel = [x for x in elements if not x['ChildQueueUrl'] and x['Status'] != 'Canceled']
            # ensure all elements receive cancel request, covers case where initial cancel request missed some elements
            # without this elements may avoid the cancel and not be cleared up till they finish
            elements_not_requested = [x for x in elements if
                                      x['ChildQueueUrl'] and (x['Status'] != 'CancelRequested' and not x.inEndState())]

            self.logger.info("Canceling work for workflow(s): %s", requestNames)
            if elements_to_cancel:
                self.backend.updateElements(*[x.id for x in elements_to_cancel], Status='Canceled')
                self.logger.info("Cancel-ed element(s) %s", str([x.id for x in elements_to_cancel]))

            if elements_not_requested:
                # Don't update as fails sometimes due to conflicts (#3856)
                for x in elements_not_requested:
                    x.load().__setitem__('Status', 'CancelRequested')
                self.backend.saveElements(*elements_not_requested)
                self.logger.info("CancelRequest-ed element(s) %s", str([x.id for x in elements_not_requested]))

            inboxElemIds = [x.id for x in inbox_elements if x['Status'] != 'CancelRequested' and not x.inEndState()]
            self.backend.updateInboxElements(*inboxElemIds, Status='CancelRequested')
            # if we haven't had any updates for a while assume agent is dead and move to canceled
            if self.params.get('cancelGraceTime', -1) > 0 and elements:
                last_update = max([float(x.updatetime) for x in elements])
                if (time.time() - last_update) > self.params['cancelGraceTime']:
                    self.logger.info("%s cancelation has stalled, mark as finished", elements[0]['RequestName'])
                    # Don't update as fails sometimes due to conflicts (#3856)
                    for x in elements:
                        if not x.inEndState():
                            x.load().__setitem__('Status', 'Canceled')
                    self.backend.saveElements(*[x for x in elements if not x.inEndState()])

        return [x.id for x in elements]

    def deleteWorkflows(self, *requests):
        """Delete requests if finished"""
        for request in requests:
            request = self.backend.getInboxElements(elementIDs=[request])
            if len(request) != 1:
                raise RuntimeError('Invalid number of requests for %s' % request[0]['RequestName'])
            request = request[0]

            if request.inEndState():
                self.logger.info('Deleting request "%s" as it is %s', request.id, request['Status'])
                self.backend.deleteElements(request)
            else:
                self.logger.debug('Not deleting "%s" as it is %s', request.id, request['Status'])

    # NOTE: this function is not executed by local workqueue
    def queueWork(self, wmspecUrl, request=None, team=None):
        """
        Take and queue work from a WMSpec.

        If request name is provided but doesn't match WMSpec name
        an error is raised.

        If team is provided work will only be available to queue's
        belonging to that team.

        Duplicate specs will be ignored.
        """
        self.logger.info('queueWork() begin queueing "%s"', wmspecUrl)
        wmspec = WMWorkloadHelper()
        wmspec.load(wmspecUrl)

        if request:  # validate request name
            if request != wmspec.name():
                raise WorkQueueWMSpecError(wmspec,
                                           'Request & workflow name mismatch %s vs %s' % (request, wmspec.name()))

        # Either pull the existing inbox element or create a new one.
        try:
            inbound = self.backend.getInboxElements(elementIDs=[wmspec.name()], loadSpec=True)
            self.logger.info('Resume splitting of "%s"', wmspec.name())
        except CouchNotFoundError:
            inbound = [self.backend.createWork(wmspec, Status='Negotiating',
                                               TeamName=team, WMBSUrl=self.params["WMBSUrl"])]
            self.backend.insertElements(inbound)

        work = self.processInboundWork(inbound, throw=True)
        return len(work)

    def addWork(self, inboundElem, rucioObj=None):
        """
        Check and add new elements to an existing running request,
        if supported by the start policy.

        :param inboundElem: dict representation for a WorkQueueElement object,
            including the WMSpec file.
        :param rucioObj: object to the Rucio class
        :return: amount of new work units added to the request
        """
        result = []
        self.logger.info('Trying to add more work for: %s', inboundElem['RequestName'])

        try:
            # Check we haven't already split the work, unless it's continuous processing
            work, rejectedWork, badWork = self._splitWork(inboundElem['WMSpec'], data=inboundElem['Inputs'],
                                                          mask=inboundElem['Mask'], inbound=inboundElem,
                                                          continuous=True, rucioObj=rucioObj)

            # if there is new work, then insert it into the database
            newWork = self.backend.insertElements(work, parent=inboundElem)

            # store the inputs in the global queue inbox workflow element
            processedInputs = []
            for unit in newWork:
                processedInputs.extend(list(unit['Inputs']))

            # update the list of processed and rejected inputs with what is already
            # defined in the workqueue inbox
            processedInputs.extend(inboundElem['ProcessedInputs'])
            rejectedWork.extend(inboundElem['RejectedInputs'])
            if newWork:
                # then also update the timestamp for when new data was found
                self.backend.updateInboxElements(inboundElem.id,
                                                 ProcessedInputs=processedInputs,
                                                 RejectedInputs=rejectedWork,
                                                 TimestampFoundNewData=int(time.time()))
            # if global queue, then update workflow stats to request mgr couch doc
            # remove the "UnittestFlag" - need to create the reqmgrSvc emulator
            if not self.params.get("UnittestFlag", False):
                # get statistics for the new work. It's already validated on the server side
                totalStats = self._getTotalStats(newWork)
                self.reqmgrSvc.updateRequestStats(inboundElem['WMSpec'].name(), totalStats)

            if badWork:
                msg = "Request with the following unprocessable input data: %s" % badWork
                self.logdb.post(inboundElem['RequestName'], msg, 'warning')
        except Exception as exc:
            self.logger.error('Generic exception adding work to WQE inbox: %s. Error: %s',
                              inboundElem, str(exc))
        else:
            result.extend(newWork)

        self.logger.info('Added %d new elements for request: %s', len(result), inboundElem['RequestName'])
        return len(result)

    def status(self, status=None, elementIDs=None,
               dictKey=None, wmbsInfo=None, loadSpec=False,
               **filters):
        """
        Return elements in the queue.

        status, elementIDs & filters are 'AND'ed together to filter elements.
        dictKey returns the output as a dict with the dictKey as the key.
        wmbsInfo causes elements to be synced with their status in WMBS.
        loadSpec causes the workflow for each spec to be loaded.
        """
        items = self.backend.getElements(status=status,
                                         elementIDs=elementIDs,
                                         loadSpec=loadSpec,
                                         **filters)

        if wmbsInfo:
            self.logger.debug("Syncing element statuses with WMBS for workflow: %s", filters.get("RequestName"))
            for item in items:
                for wmbs in wmbsInfo:
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

    def getWMBSSubscriptionStatus(self):
        """
        Fetches all the subscriptions in this agent and make a summary of
        every single one of them, to be used to update WQEs
        :return: a list of dictionaries
        """
        from WMCore.WorkQueue.WMBSHelper import wmbsSubscriptionStatus
        self.logger.info("Fetching WMBS subscription status information")
        wmbsStatus = wmbsSubscriptionStatus(logger=self.logger,
                                            dbi=self.conn.dbi,
                                            conn=self.conn.getDBConn(),
                                            transaction=self.conn.existingTransaction())
        return wmbsStatus

    def statusInbox(self, status=None, elementIDs=None, dictKey=None, **filters):
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
        self.logger.info('Executing data location update...')
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: skipping location update')
            return 0
        result = self.dataLocationMapper()
        self.backend.recordTaskActivity('location_refresh')
        return result

    def _printLog(self, msg, printFlag, logLevel):
        if printFlag:
            print(msg)
        else:
            getattr(self.logger, logLevel)(msg)

    def pullWorkConditionCheck(self, printFlag=False):

        if not self.params['ParentQueueCouchUrl']:
            msg = 'Unable to pull work from parent, ParentQueueCouchUrl not provided'
            self._printLog(msg, printFlag, "warning")
            return False
        if not self.backend.isAvailable() or not self.parent_queue.isAvailable():
            msg = 'Backend busy or down: skipping work pull'
            self._printLog(msg, printFlag, "warning")
            return False

        left_over = self.parent_queue.getElements('Negotiating', returnIdOnly=True,
                                                  ChildQueueUrl=self.params['QueueURL'])
        if left_over:
            msg = 'Not pulling more work. Still replicating %d previous units, ids:\n%s' % (len(left_over), left_over)
            self._printLog(msg, printFlag, "warning")
            return False

        still_processing = self.backend.getInboxElements('Negotiating', returnIdOnly=True)
        if still_processing:
            msg = 'Not pulling more work. Still processing %d previous units' % len(still_processing)
            self._printLog(msg, printFlag, "warning")
            return False

        return True

    def freeResouceCheck(self):
        """
        This method looks into the WMBS and BossAir tables and collect
        two types of information:
         1) sites and the total slots available for job creation
         2) sites and the number of pending jobs grouped by priority
        With that information in hands, it looks at the local workqueue elements
        sitting in Available status and update the 2nd data structure (thus it
        updates number of jobs pending by priority according to the LQEs), which
        is then used to know which work can be acquired from the parent queue or not.
        :return: a tuple of dictionaries (or empty lists)
        """
        from WMCore.WorkQueue.WMBSHelper import freeSlots
        resources, jobCounts = freeSlots(self.params['QueueDepth'], knownCmsSites=cmsSiteNames())
        # now update jobCounts with work that is already available in the local queue
        _, jobCounts = self.backend.calculateAvailableWork(resources, jobCounts)

        return (resources, jobCounts)

    def getAvailableWorkfromParent(self, resources, jobCounts, printFlag=False):
        numElems = self.params['WorkPerCycle']
        self.logger.info("Going to fetch work from the parent queue: %s", self.parent_queue.queueUrl)
        work, _ = self.parent_queue.availableWork(resources, jobCounts, self.params['Team'], numElems=numElems)
        if not work:
            self._printLog('No available work in parent queue.', printFlag, "warning")
        return work

    def pullWork(self, resources=None):
        """
        Pull work from another WorkQueue to be processed:
        :param resources: optional dictionary with sites and the amount
        of slots free
        """
        jobCounts = {}
        if self.pullWorkConditionCheck() is False:
            return 0

        # NOTE: resources parameter is only used by unit tests, which do
        # not use WMBS and BossAir tables
        if not resources:
            (resources, jobCounts) = self.freeResouceCheck()
        if not resources and not jobCounts:
            return 0

        work = self.getAvailableWorkfromParent(resources, jobCounts)
        if not work:
            return 0

        work = self._assignToChildQueue(self.params['QueueURL'], *work)

        return len(work)

    def closeWork(self):
        """
        Global queue service that looks for the inbox elements that are still active
        and checks whether they should be closed for new data or not.
        An element is closed automatically when one of the following conditions holds true:
        - The StartPolicy doesn't define a OpenRunningTimeout or this delay is set to 0
        - A period longer than OpenRunningTimeout has passed since the last child element
           was created or an open block was found and the StartPolicy newDataAvailable
           function returns False.

        :return: list of workqueue_inbox elements that have been closed
        """
        workflowsToClose = []
        if self.params['LocalQueueFlag']:
            # this is a Global WorkQueue only functionality
            return workflowsToClose
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: Can not close work at this time')
            return workflowsToClose

        workflowsToCheck = self.backend.getInboxElements(OpenForNewData=True)
        self.logger.info("Retrieved a list of %d open workflows", len(workflowsToCheck))
        currentTime = time.time()
        for element in workflowsToCheck:
            # fetch attributes from the inbox workqueue element
            startPol = element.get('StartPolicy', {})
            openRunningTimeout = startPol.get('OpenRunningTimeout', 0)
            foundNewDataTime = element.get('TimestampFoundNewData', 0)
            if not openRunningTimeout:
                self.logger.info("Workflow %s has no OpenRunningTimeout. Queuing to be closed.",
                                 element['RequestName'])
                workflowsToClose.append(element.id)
            elif (currentTime - foundNewDataTime) > openRunningTimeout:
                # then it's been too long since the last element has been found
                self.logger.info("Workflow %s has expired OpenRunningTimeout. Queuing to be closed.",
                                 element['RequestName'])
                workflowsToClose.append(element.id)

        if workflowsToClose:
            try:
                self.logger.info('Closing workflows in workqueue_inbox for: %s', workflowsToClose)
                self.backend.updateInboxElements(*workflowsToClose, OpenForNewData=False)
                msg = 'Closed inbox elements for: %s.\n' % ', '.join(workflowsToClose)
            except CouchInternalServerError as ex:
                msg = 'Failed to close workflows with a CouchInternalServerError exception. '
                msg += 'Details: {}'.format(str(ex))
                self.logger.error(msg)
            except Exception as ex:
                msg = 'Failed to close workflows with a generic exception. '
                msg += 'Details: {}'.format(str(ex))
                self.logger.exception(msg)
        else:
            msg = 'No workflows to close.\n'

        self.backend.recordTaskActivity('workclosing', msg)

        return workflowsToClose

    def deleteCompletedWFElements(self):
        """
        deletes Workflow when workflow is in finished status
        """
        deletableStates = ["completed", "closed-out", "failed",
                           "announced", "aborted-completed", "rejected",
                           "normal-archived", "aborted-archived", "rejected-archived"]

        # fetch workflows known to workqueue + workqueue_inbox and with spec attachments
        reqNames = self.backend.getWorkflows(includeInbox=True, includeSpecs=True)
        self.logger.info("Retrieved %d workflows known by WorkQueue", len(reqNames))
        requestsInfo = self.requestDB.getRequestByNames(reqNames)
        deleteRequests = []
        for key, value in viewitems(requestsInfo):
            if (value["RequestStatus"] is None) or (value["RequestStatus"] in deletableStates):
                deleteRequests.append(key)
        self.logger.info("Found %d out of %d workflows in a deletable state",
                         len(deleteRequests), len(reqNames))
        return self.backend.deleteWQElementsByWorkflow(deleteRequests)

    def performSyncAndCancelAction(self, skipWMBS):
        """
        Apply end policies to determine work status & cleanup finished work
        """
        if not self.backend.isAvailable():
            self.logger.warning('Backend busy or down: skipping cleanup tasks')
            return

        if self.params['LocalQueueFlag']:
            self.backend.fixConflicts()  # before doing anything fix any conflicts

        wf_to_cancel = []  # record what we did for task_activity
        finished_elements = []

        useWMBS = not skipWMBS and self.params['LocalQueueFlag']
        if useWMBS:
            wmbsWflowSummary = self.getWMBSSubscriptionStatus()
        else:
            wmbsWflowSummary = []
        # Get queue elements grouped by their workflow with updated wmbs progress
        # Cancel if requested, update locally and remove obsolete elements
        self.logger.info('Fetching workflow information (including inbox and specs)')
        workflowsList = self.backend.getWorkflows(includeInbox=True, includeSpecs=True)
        for wf in workflowsList:
            parentQueueDeleted = True
            try:
                elements = self.status(RequestName=wf, wmbsInfo=wmbsWflowSummary)
                parents = self.backend.getInboxElements(RequestName=wf)

                self.logger.debug("Queue %s status follows:", self.backend.queueUrl)
                results = endPolicy(elements, parents, self.params['EndPolicySettings'])
                for result in results:
                    self.logger.debug("Request %s, Status %s, Full info: %s",
                                      result['RequestName'], result['Status'], result)

                    # check for cancellation requests (affects entire workflow)
                    if result['Status'] == 'CancelRequested':
                        self.logger.info('Canceling work for workflow: %s', wf)
                        canceled = self.cancelWork(WorkflowName=wf)
                        if canceled:  # global wont cancel if work in child queue
                            wf_to_cancel.append(wf)
                            break
                    elif result['Status'] == 'Negotiating':
                        self.logger.debug("Waiting for %s to finish splitting", wf)
                        continue

                    parent = result['ParentQueueElement']
                    if parent.modified:
                        self.backend.saveElements(parent)

                    if result.inEndState():
                        if elements:
                            self.logger.debug("Request %s finished (%s)",
                                              result['RequestName'], parent.statusMetrics())
                            finished_elements.extend(result['Elements'])
                        else:
                            parentQueueDeleted = False
                        continue

                    updated_elements = [x for x in result['Elements'] if x.modified]
                    for x in updated_elements:
                        self.logger.debug("Updating progress %s (%s): %s", x['RequestName'], x.id, x.statusMetrics())
                        self.backend.updateElements(x.id, **x.statusMetrics())

                if not parentQueueDeleted:
                    self.logger.info('Waiting for parent queue to delete "%s"', wf)

            except Exception as ex:
                self.logger.error('Error processing workflow "%s": %s', wf, str(ex))

        msg = 'Finished elements: %s\nCanceled workflows: %s' % (', '.join(["%s (%s)" % (x.id, x['RequestName']) \
                                                                            for x in finished_elements]),
                                                                 ', '.join(wf_to_cancel))

        self.logger.debug(msg)
        self.backend.recordTaskActivity('housekeeping', msg)

    def performQueueCleanupActions(self, skipWMBS=False):

        try:
            self.logger.info("Deleting completed workflow WQ elements ...")
            res = self.deleteCompletedWFElements()
            self.logger.info("Deleted %d elements from workqueue/inbox database", res)
        except Exception as ex:
            self.logger.exception('Error deleting WQ elements. Details: %s', str(ex))

        try:
            self.logger.info("Syncing and cancelling work ...")
            self.performSyncAndCancelAction(skipWMBS)
        except Exception as ex:
            self.logger.error('Error syncing and canceling WQ elements. Details: %s', str(ex))

    def _splitWork(self, wmspec, data=None, mask=None, inbound=None, continuous=False, rucioObj=None):
        """
        Split work from a parent into WorkQeueueElements.

        If data param supplied use that rather than getting input data from
        wmspec. Used for instance when global splits by Block (avoids having to
        modify wmspec block whitelist - thus all appear as same wf in wmbs)

        mask can be used to specify i.e. event range.

        The inbound and continuous parameters are used to split
        and already split inbox element.
        """
        # give preference to rucio object created by the CherryPy threads
        if not rucioObj:
            rucioObj = self.rucio

        totalUnits = []
        # split each top level task into constituent work elements
        # get the acdc server and db name
        for topLevelTask in wmspec.taskIterator():
            spec = getWorkloadFromTask(topLevelTask)
            policyName = spec.startPolicy()
            if not policyName:
                raise RuntimeError("WMSpec doesn't define policyName, current value: '%s'" % policyName)

            policy = startPolicy(policyName, self.params['SplittingMapping'],
                                 rucioObj=rucioObj, logger=self.logger)
            if not policy.supportsWorkAddition() and continuous:
                # Can't split further with a policy that doesn't allow it
                continue
            if continuous:
                policy.modifyPolicyForWorkAddition(inbound)
            self.logger.info('Splitting %s with policy name %s and policy params %s',
                             topLevelTask.getPathName(), policyName,
                             self.params['SplittingMapping'].get(policyName))
            units, rejectedWork, badWork = policy(spec, topLevelTask, data, mask, continuous=continuous)
            self.logger.info('Work splitting completed with %d units, %d rejectedWork and %d badWork',
                             len(units), len(rejectedWork), len(badWork))
            for unit in units:
                msg = 'Queuing element {} for {} with policy {}, '.format(unit.id, unit['Task'].getPathName(),
                                                                          unit['StartPolicy'])
                msg += 'with {} job(s) and {} lumis'.format(unit['Jobs'], unit['NumberOfLumis'])
                if unit['Inputs']:
                    msg += ' on %s' % list(unit['Inputs'])[0]
                if unit['Mask']:
                    msg += ' on events %d-%d' % (unit['Mask']['FirstEvent'], unit['Mask']['LastEvent'])
                self.logger.info(msg)
            totalUnits.extend(units)

        return (totalUnits, rejectedWork, badWork)

    def _getTotalStats(self, units):
        totalToplevelJobs = 0
        totalEvents = 0
        totalLumis = 0
        totalFiles = 0

        for unit in units:
            totalToplevelJobs += unit['Jobs']
            totalEvents += unit['NumberOfEvents']
            totalLumis += unit['NumberOfLumis']
            totalFiles += unit['NumberOfFiles']

        return {'total_jobs': totalToplevelJobs,
                'input_events': totalEvents,
                'input_lumis': totalLumis,
                'input_num_files': totalFiles}

    def processInboundWork(self, inbound_work=None, throw=False, continuous=False, rucioObj=None):
        """Retrieve work from inbox, split and store
        If request passed then only process that request
        """
        inbound_work = inbound_work or []
        msg = "Executing processInboundWork with {} inbound_work, ".format(len(inbound_work))
        msg += "throw: {} and continuous: {}".format(throw, continuous)
        self.logger.info(msg)
        if self.params['LocalQueueFlag']:
            self.logger.info("fixing conflict...")
            self.backend.fixConflicts()  # db should be consistent

        result = []
        if not inbound_work and continuous:
            # This is not supported
            return result
        if not inbound_work:
            inbound_work = self.backend.getElementsForSplitting()
            self.logger.info('Retrieved %d elements for splitting with continuous flag: %s',
                             len(inbound_work), continuous)
        for inbound in inbound_work:
            try:
                # Check we haven't already split the work, unless it's continuous processing
                work = not continuous and self.backend.getElementsForParent(inbound)
                if work:
                    self.logger.info('Request "%s" already split - Resuming', inbound['RequestName'])
                else:
                    work, rejectedWork, badWork = self._splitWork(inbound['WMSpec'], data=inbound['Inputs'],
                                                                  mask=inbound['Mask'], inbound=inbound,
                                                                  continuous=continuous, rucioObj=rucioObj)

                    # save inbound work to signal we have completed queueing
                    # if this fails, rerunning will pick up here
                    newWork = self.backend.insertElements(work, parent=inbound)
                    # get statistics for the new work
                    totalStats = self._getTotalStats(newWork)

                    if not continuous:
                        # Update to Acquired when it's the first processing of inbound work
                        self.backend.updateInboxElements(inbound.id, Status='Acquired')

                    # store the inputs in the global queue inbox workflow element
                    if not self.params.get('LocalQueueFlag'):
                        processedInputs = []
                        for unit in work:
                            processedInputs.extend(list(unit['Inputs']))
                        self.backend.updateInboxElements(inbound.id, ProcessedInputs=processedInputs,
                                                         RejectedInputs=rejectedWork)
                        # if global queue, then update workflow stats to request mgr couch doc
                        # remove the "UnittestFlag" - need to create the reqmgrSvc emulator
                        if not self.params.get("UnittestFlag", False):
                            self.reqmgrSvc.updateRequestStats(inbound['WMSpec'].name(), totalStats)

                    if badWork:
                        msg = "Request with the following unprocessable input data: %s" % badWork
                        self.logdb.post(inbound['RequestName'], msg, 'warning')
            except TERMINAL_EXCEPTIONS as ex:
                msg = 'Terminal exception splitting WQE: %s' % inbound
                self.logger.error(msg)
                self.logdb.post(inbound['RequestName'], msg, 'error')
                if not continuous:
                    # Only fail on first splitting
                    self.logger.error('Failing workflow "%s": %s', inbound['RequestName'], str(ex))
                    self.backend.updateInboxElements(inbound.id, Status='Failed')
                    if throw:
                        raise
            except Exception as ex:
                if continuous:
                    continue
                msg = 'Exception splitting wqe %s for %s: %s' % (inbound.id, inbound['RequestName'], str(ex))
                self.logger.exception(msg)
                self.logdb.post(inbound['RequestName'], msg, 'error')

                if throw:
                    raise
                continue
            else:
                result.extend(work)

        requests = ', '.join(list(set(['"%s"' % x['RequestName'] for x in result])))
        if requests:
            self.logger.info('Split work for request(s): %s', requests)

        return result

    def getWMBSInjectionStatus(self, workflowName=None, drainMode=False):
        """
        if the parent queue exist return the result from parent queue.
        other wise return the result from the current queue.
        (In general parent queue always exist when it is called from local queue
        except T1 skim case)
        returns list of [{workflowName: injection status (True or False)}]
        if the workflow is not exist return []
        """
        if self.parent_queue and not drainMode:
            return self.parent_queue.getWMBSInjectStatus(workflowName)
        return self.backend.getWMBSInjectStatus(workflowName)

    def monitorWorkQueue(self, status=None):
        """
        Uses the workqueue data-service to retrieve a few basic information
        regarding all the elements in the queue.
        """
        status = status or []
        results = {}
        start = int(time.time())
        results['workByStatus'] = self.workqueueDS.getJobsByStatus()
        results['workByStatusAndPriority'] = self.workqueueDS.getJobsByStatusAndPriority()
        results['workByAgentAndStatus'] = self.workqueueDS.getChildQueuesAndStatus()
        results['workByAgentAndPriority'] = self.workqueueDS.getChildQueuesAndPriority()

        # now the heavy procesing for the site information
        elements = self.workqueueDS.getElementsByStatus(status)
        uniSites, posSites = getGlobalSiteStatusSummary(elements, status=status)
        results['uniqueJobsPerSiteAAA'] = uniSites
        results['possibleJobsPerSiteAAA'] = posSites
        uniSites, posSites = getGlobalSiteStatusSummary(elements, status=status, dataLocality=True)
        results['uniqueJobsPerSite'] = uniSites
        results['possibleJobsPerSite'] = posSites

        end = int(time.time())
        results["total_query_time"] = end - start
        return results
