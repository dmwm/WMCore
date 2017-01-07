import json
from collections import defaultdict
from WMCore.Database.CMSCouch import CouchServer
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper


# TODO: this could be derived from the Service class to use client side caching
class WorkQueue(object):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName = None, inboxDBName = None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.hostWithAuth = couchURL
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create = False)
        if not inboxDBName:
            inboxDBName = "%s_inbox" % dbName
        self.inboxDB = self.server.connectDatabase(inboxDBName, create = False)
        self.defaultOptions = {'stale': "update_after", 'reduce' : True, 'group' : True}

    def getTopLevelJobsByRequest(self):
        """Get data items we have work in the queue for"""
    
        data = self.db.loadView('WorkQueue', 'jobsByRequest', self.defaultOptions)
        return [{'request_name' : x['key'],
                 'total_jobs' : x['value']} for x in data.get('rows', [])]

    def getChildQueues(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'childQueues', self.defaultOptions)
        return [x['key'] for x in data.get('rows', [])]

    def getChildQueuesByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'childQueuesByRequest',
                                self.defaultOptions)
        return [{'request_name' : x['key'][0],
                 'local_queue' : x['key'][1]} for x in data.get('rows', [])]

    def getWMBSUrl(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrl', self.defaultOptions)
        return [x['key'] for x in data.get('rows', [])]

    def getWMBSUrlByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrlByRequest', self.defaultOptions)
        return [{'request_name' : x['key'][0],
                 'wmbs_url' : x['key'][1]} for x in data.get('rows', [])]

    def getJobStatusByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'jobStatusByRequest',
                                self.defaultOptions)
        return [{'request_name' : x['key'][0], 'status': x['key'][1],
                 'jobs' : x['value']} for x in data.get('rows', [])]

    def getJobInjectStatusByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'jobInjectStatusByRequest',
                                self.defaultOptions)
        return [{'request_name' : x['key'][0], x['key'][1]: x['value']}
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
        return [{'request_name' : x['key'][0], 'site_whitelist': x['key'][1]}
                for x in data.get('rows', [])]

    def updateElements(self, *elementIds, **updatedParams):
        """Update given element's (identified by id) with new parameters"""
        if not elementIds:
            return
        import urllib
        uri = "/" + self.db.name + "/_design/WorkQueue/_update/in-place/"
        optionsArg = {}
        if "options" in updatedParams:
            optionsArg.update(updatedParams.pop("options"))
        data = {"updates" : json.dumps(updatedParams),
                "options" : json.dumps(optionsArg)}
        for ele in elementIds:
            thisuri = uri + ele + "?" + urllib.urlencode(data)
            self.db.makeRequest(uri = thisuri, type = 'PUT')
        return

    def getAvailableWorkflows(self):
        """Get the workflows that have all their elements
           available in the workqueue"""
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'reduce' : False, 'stale': 'update_after'})
        availableSet = set((x['value']['RequestName'], x['value']['Priority']) for x in data.get('rows', []) if x['key'][1] == 'Available')
        notAvailableSet = set((x['value']['RequestName'], x['value']['Priority']) for x in data.get('rows', []) if x['key'][1] != 'Available')
        return availableSet - notAvailableSet

    def cancelWorkflow(self, wf):
        """Cancel a workflow"""
        nonCancelableElements = ['Done', 'Canceled', 'Failed']
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'startkey' : [wf], 'endkey' : [wf, {}],
                                 'reduce' : False})
        elements = [x['id'] for x in data.get('rows', []) if x['key'][1] not in nonCancelableElements]
        return self.updateElements(*elements, Status = 'CancelRequested')

    def updatePriority(self, wf, priority):
        """Update priority of a workflow, this implies
           updating the spec and the priority of the Available elements"""
        # Update elements in Available status
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'startkey' : [wf], 'endkey' : [wf, {}],
                                 'reduce' : False})
        elementsToUpdate = [x['id'] for x in data.get('rows', [])]
        if elementsToUpdate:
            self.updateElements(*elementsToUpdate, Priority = priority)
        # Update the spec, if it exists
        if self.db.documentExists(wf):
            wmspec = WMWorkloadHelper()
            wmspec.load(self.db['host'] + "/%s/%s/spec" % (self.db.name, wf))
            wmspec.setPriority(priority)
            dummy_values = {'name' : wmspec.name()}
            wmspec.saveCouch(self.hostWithAuth, self.db.name, dummy_values)
        return

    def getWorkflowNames(self, inboxFlag = False):
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

    def getElementsStatusAndJobsByWorkflow(self, inboxFlag=False, stale=True):
        """Get the number of elements and jobs by status and workflow"""
        if inboxFlag:
            db = self.inboxDB
        else:
            db = self.db
        options = {'reduce': True, 'group_level': 2}
        if stale:
            options['stale'] = 'update_after'
        data = db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus', options)
        result = defaultdict(dict)
        for x in data.get('rows', []):
            result[x['key'][0]][x['key'][1]] = {'NumOfElements': x['value']['count'], 
                                                'Jobs': x['value']['totalJobs']}
        return result
    
    def _getCompletedWorkflowList(self, data):
        completedWFs = []
        for workflow in data:
            completed = True
            for status in data[workflow]:
                if status not in ['Done', 'Failed', 'Canceled']:
                    completed = False
            if completed:
                completedWFs.append(workflow)
        return completedWFs
    
    def getCompletedWorkflow(self, stale=True):
        """
        only checks workqueue db not inbox db. 
        since inbox db will be cleaned up first when workflow is completed
        """
        data = self.getElementsStatusAndJobsByWorkflow(stale)
        return self._getCompletedWorkflowList(data)

    def getJobsByStatus(self, inboxFlag=False, group=True):
        """
        Returns some stats for the workqueue elements in each status, like:
         1. total number of expected Jobs
         2. count of elements
         3. minimum number of expected Jobs in an element
         4. maximum number of expected Jobs in an element
         5. sum of the squares of the expected Job in each element

        Provide group=False in order to get a final summary of all the elements.
        """
        if inboxFlag:
            db = self.inboxDB
        else:
            db = self.db
        options = {'reduce': True, 'group': group, 'stale': 'update_after'}

        data = db.loadView('WorkQueue', 'jobsByStatus', options)
        result = {}
        for x in data.get('rows', []):
            result[x['key']] = x['value']

        return result
