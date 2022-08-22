from builtins import str, bytes, object
from future.utils import viewitems

from collections import defaultdict
from WMCore.Database.CMSCouch import CouchServer, CouchConflictError
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper
from WMCore.WorkQueue.DataStructs.WorkQueueElement import STATES


def convertWQElementsStatusToWFStatus(elementsStatusSet):
    """
    Defined Workflow status from its WorkQeuueElement status.
    :param: elementsStatusSet - dictionary of {request_name: set of all WQE status of this request, ...}
    :returns: request status

    Here is the mapping between request status and the GQE status
    1. acquired:  all the GQEs are either Available or Negotiating.
        Work is still in GQ, but not LQ.
    2. running-open: at least one of the GQEs are in Acquired status.
    3. running-closed: all the GQEs are in Running or beyond status.
        No Available/Negotiating/Acquired status, all the work is in WMBS db (in agents)
    4. completed: all the GQEs are in a final status, like Done/Canceled/Failed.
        All work is finished in WMBS (excluding cleanup and logcollect)
    5. failed: all the GQEs are in Failed status. If the workflow has multiple GQEs
        and only a few are in Failed status, then just follow the usual request status.
    6. canceled: used to distinguish requests that have been correctly canceled,
        coming from workflows either aborted or force-complete. This state does not
        trigger a workflow status transition.
    """
    if not elementsStatusSet:
        return None

    forceCompleted = set(["CancelRequested"])
    available = set(["Available", "Negotiating", "Failed"])
    acquired = set(["Acquired"])
    running = set(["Running"])
    runningOpen = set(["Available", "Negotiating", "Acquired"])
    runningClosed = set(["Running", "Done", "Canceled"])
    canceled = set(["CancelRequested", "Done", "Canceled", "Failed"])
    completed = set(["Done", "Canceled", "Failed"])
    failed = set(["Failed"])

    # Just a reminder:
    # <= every element in the left set is also in the right set
    # & return elements common between the left and right set
    if elementsStatusSet == forceCompleted:  # all WQEs in CancelRequested
        return "canceled"
    elif elementsStatusSet == acquired:  # all WQEs in Acquired
        return "running-open"
    elif elementsStatusSet == running:  # all WQEs in Running
        return "running-closed"
    elif elementsStatusSet == failed:  # all WQEs in Failed
        return "failed"
    elif elementsStatusSet <= available:  # all WQEs still in GQ
        return "acquired"
    elif elementsStatusSet <= completed:  # all WQEs in a final state
        return "completed"
    elif elementsStatusSet <= canceled:  # some WQEs still waiting to be cancelled
        return "canceled"
    elif elementsStatusSet & runningOpen:  # at least 1 WQE still in GQ
        return "running-open"
    elif elementsStatusSet & runningClosed:  # all WQEs already in LQ and WMBS
        return "running-closed"
    else:
        # transitional status. Negotiating status won't be changed.
        return None


# TODO: this could be derived from the Service class to use client side caching
class WorkQueue(object):
    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName=None, inboxDBName=None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.hostWithAuth = couchURL
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create=False)
        if not inboxDBName:
            inboxDBName = "%s_inbox" % dbName
        self.inboxDB = self.server.connectDatabase(inboxDBName, create=False)
        self.defaultOptions = {'stale': "update_after", 'reduce': True, 'group': True}
        self.eleKey = 'WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'
        self.states = STATES

    def getTopLevelJobsByRequest(self):
        """Get data items we have work in the queue for"""

        data = self.db.loadView('WorkQueue', 'jobsByRequest', self.defaultOptions)
        return [{'request_name': x['key'],
                 'total_jobs': x['value']} for x in data.get('rows', [])]

    def getChildQueuesAndStatus(self, stale=True):
        """
        Returns some stats for the workqueue elements in each ChildQueue and their status.
         1. total number of expected Jobs (sum)
         2. count of elements (count)
         3. minimum number of expected Jobs in an element
         4. maximum number of expected Jobs in an element
         5. sum of the squares of the expected Job in each element

        Also reformat the output such that it's MONIT IT friendly and easier to aggregate.
        """
        options = {'reduce': True, 'group_level': 2}
        if stale:
            options['stale'] = 'update_after'

        data = self.db.loadView('WorkQueue', 'jobsByChildQueueAndStatus', options)
        result = []
        for x in data.get('rows', []):
            item = {'agent_name': self._getShortName(x['key'][0]),
                    'status': x['key'][1]}
            item.update(dict(sum_jobs=x['value']['sum'],
                             num_elem=x['value']['count'],
                             max_jobs_elem=x['value']['max']))
            result.append(item)

        return result

    def getChildQueuesAndPriority(self, stale=True):
        """
        Returns some stats for the workqueue elements in each ChildQueue and their priority.
        """
        options = {'reduce': True, 'group_level': 2}
        if stale:
            options['stale'] = 'update_after'

        data = self.db.loadView('WorkQueue', 'jobsByChildQueueAndPriority', options)
        result = []
        for x in data.get('rows', []):
            item = {'agent_name': self._getShortName(x['key'][0]),
                    'priority': int(x['key'][1])}
            item.update(dict(sum_jobs=x['value']['sum'],
                             num_elem=x['value']['count'],
                             max_jobs_elem=x['value']['max']))
            result.append(item)

        return result

    def _getShortName(self, longQueueName):
        """
        Get a full workqueue queue name (full hostname + port) and return its short name,
        otherwise it fails to get injected into elastic search. E.g.:
            from "http://cmssrv217.fnal.gov:5984" to "cmssrv217"
        """
        if longQueueName is None:
            return "AgentNotDefined"
        shortName = longQueueName.split('//')[-1]
        shortName = shortName.split('.')[0]
        return shortName

    def getWMBSUrl(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrl', self.defaultOptions)
        return [x['key'] for x in data.get('rows', [])]

    def getWMBSUrlByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrlByRequest', self.defaultOptions)
        return [{'request_name': x['key'][0],
                 'wmbs_url': x['key'][1]} for x in data.get('rows', [])]

    def getJobInjectStatusByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'jobInjectStatusByRequest',
                                self.defaultOptions)
        return [{'request_name': x['key'][0], x['key'][1]: x['value']}
                for x in data.get('rows', [])]

    def getAnalyticsData(self):
        """
        This getInject status and input dataset from workqueue
        """
        results = self.db.loadView('WorkQueue', 'jobInjectStatusByRequest',
                                   self.defaultOptions)
        statusByRequest = {}
        for x in results.get('rows', []):
            statusByRequest.setdefault(x['key'][0], {})
            statusByRequest[x['key'][0]][x['key'][1]] = x['value']

        return statusByRequest

    def getSiteWhitelistByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'siteWhitelistByRequest',
                                self.defaultOptions)
        return [{'request_name': x['key'][0], 'site_whitelist': x['key'][1]}
                for x in data.get('rows', [])]

    def updateElements(self, *elementIds, **updatedParams):
        """Update given element's (identified by id) with new parameters"""
        if not elementIds:
            return
        eleParams = {}
        eleParams[self.eleKey] = updatedParams
        conflictIDs = self.db.updateBulkDocumentsWithConflictHandle(elementIds, eleParams, maxConflictLimit=20)
        if conflictIDs:
            raise CouchConflictError("WQ update failed with conflict", data=updatedParams, result=conflictIDs)
        return

    def getAvailableWorkflows(self):
        """Get the workflows that have all their elements
           available in the workqueue"""
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'reduce': False, 'stale': 'update_after'})
        availableSet = set((x['value']['RequestName'], x['value']['Priority']) for x in data.get('rows', []) if
                           x['key'][1] == 'Available')
        notAvailableSet = set((x['value']['RequestName'], x['value']['Priority']) for x in data.get('rows', []) if
                              x['key'][1] != 'Available')
        return availableSet - notAvailableSet

    def cancelWorkflow(self, wf):
        """Cancel a workflow"""
        nonCancelableElements = ['Done', 'CancelRequested', 'Canceled', 'Failed']
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'startkey': [wf], 'endkey': [wf, {}],
                                 'reduce': False})
        elements = [x['id'] for x in data.get('rows', []) if x['key'][1] not in nonCancelableElements]
        return self.updateElements(*elements, Status='CancelRequested')

    def updatePriority(self, wf, priority):
        """Update priority of a workflow, this implies
           updating the spec and the priority of the Available elements"""
        # Update elements in Available status
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'startkey': [wf], 'endkey': [wf, {}],
                                 'reduce': False})
        elementsToUpdate = [x['id'] for x in data.get('rows', [])]
        if elementsToUpdate:
            self.updateElements(*elementsToUpdate, Priority=priority)
        # Update the spec, if it exists
        if self.db.documentExists(wf):
            wmspec = WMWorkloadHelper()
            wmspec.load(self.hostWithAuth + "/%s/%s/spec" % (self.db.name, wf))
            wmspec.setPriority(priority)
            dummy_values = {'name': wmspec.name()}
            wmspec.saveCouch(self.hostWithAuth, self.db.name, dummy_values)
        return

    def getWorkflowNames(self, inboxFlag=False):
        """Get workflow names from workqueue db"""
        if inboxFlag:
            db = self.inboxDB
        else:
            db = self.db
        data = db.loadView('WorkQueue', 'elementsByWorkflow', self.defaultOptions)
        return [x['key'] for x in data.get('rows', [])]

    def deleteWQElementsByWorkflow(self, workflowNames):
        """
        delete workqueue elements belongs to given workflow names
        """
        deleted = 0
        dbs = [self.db, self.inboxDB]
        if not isinstance(workflowNames, list):
            workflowNames = [workflowNames]

        if len(workflowNames) == 0:
            return deleted

        options = {}
        options["stale"] = "ok"
        options["reduce"] = False

        for couchdb in dbs:
            result = couchdb.loadView("WorkQueue", "elementsByWorkflow", options, workflowNames)
            ids = []
            for entry in result["rows"]:
                ids.append(entry["id"])
            if ids:
                couchdb.bulkDeleteByIDs(ids)
                deleted += len(ids)
        return deleted

    def getElementsCountAndJobsByWorkflow(self, inboxFlag=False, stale=True):
        """Get the number of elements and jobs by status and workflow"""
        if inboxFlag:
            db = self.inboxDB
        else:
            db = self.db
        options = {'reduce': True, 'group_level': 2}
        if stale:
            options['stale'] = 'update_after'
        data = db.loadView('WorkQueue', 'jobStatusByRequest', options)
        result = defaultdict(dict)
        for x in data.get('rows', []):
            result[x['key'][0]][x['key'][1]] = {'NumOfElements': x['value']['count'],
                                                'Jobs': x['value']['sum']}
        return result


    def _retrieveWorkflowStatus(self, data):
        workflowsStatus = {}

        for workflow in data:
            statusSet = set(data[workflow].keys())
            status = convertWQElementsStatusToWFStatus(statusSet)
            if status:
                workflowsStatus[workflow] = status
        return workflowsStatus


    def getWorkflowStatusFromWQE(self, stale=True):
        """
        only checks workqueue db not inbox db.
        returns and list of workflows by request status
        """
        data = self.getElementsCountAndJobsByWorkflow(stale=stale)
        return self._retrieveWorkflowStatus(data)

    def getCompletedWorkflow(self, stale=True):
        """
        only checks workqueue db not inbox db.
        since inbox db will be cleaned up first when workflow is completed
        """
        workflowStatus = self.getWorkflowStatusFromWQE(stale=stale)
        return [wf for wf, status in viewitems(workflowStatus) if status == "completed"]

    def getJobsByStatus(self, inboxFlag=False, group=True):
        """
        For each WorkQueue element status, returns:
         * total number of jobs (sum)
         * number of workqueue elements (count)
         * biggest number of jobs found in all those elements (max)

        Use group=False in order to get a final summary of all the elements.
        """
        if inboxFlag:
            db = self.inboxDB
        else:
            db = self.db
        options = {'reduce': True, 'group': group, 'stale': 'update_after'}

        data = db.loadView('WorkQueue', 'jobsByStatus', options)
        result = {}
        # Add all WorkQueueElement status to the output
        for st in self.states:
            result[st] = {}

        for x in data.get('rows', []):
            item = dict(sum_jobs=x['value']['sum'],
                        num_elem=x['value']['count'],
                        max_jobs_elem=x['value']['max'])
            result[x['key']] = item

        return result

    def getJobsByStatusAndPriority(self, stale=True):
        """
        For each WorkQueue element status, returns a list of:
         * workqueue element priority
         * total number of jobs (sum)
         * number of workqueue elements (count)
         * biggest number of jobs found in all those elements (max)
        """
        options = {'reduce': True, 'group_level': 2}
        if stale:
            options['stale'] = 'update_after'

        data = self.db.loadView('WorkQueue', 'jobsByStatusAndPriority', options)
        result = {}
        # Add all WorkQueueElement status to the output
        for st in self.states:
            result[st] = []

        for x in data.get('rows', []):
            st = x['key'][0]
            prio = x['key'][1]
            item = dict(priority=int(prio), sum_jobs=x['value']['sum'],
                        num_elem=x['value']['count'], max_jobs_elem=x['value']['max'])
            result[st].append(item)

        return result

    def getElementsByStatus(self, status, inboxFlag=False, stale=True):
        """
        _getElementsByStatus_

        Returns the whole elements in workqueue that match the list of status given.
        """
        if isinstance(status, (str, bytes)):
            status = [status]

        options = {'stale': 'update_after'} if stale else {}
        options['include_docs'] = True

        db = self.inboxDB if inboxFlag else self.db

        data = db.loadView('WorkQueue', 'elementsByStatus', options, status)
        result = {}
        for x in data.get('rows', []):
            # doc may have been deleted already
            if x['doc']:
                result.setdefault(x['key'], [])
                result[x['key']].append(x['doc'])

        return result
