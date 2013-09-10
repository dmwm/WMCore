from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.WMSpec.WMWorkload import WMWorkloadHelper

# TODO: this could be derived from the Service class to use client side caching
class WorkQueue(object):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName = None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.hostWithAuth = couchURL
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create = False)
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
            answer = self.db.makeRequest(uri = thisuri, type = 'PUT')
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
        elementsToUpdate = [x['id'] for x in data.get('rows', []) if x['key'][1] == 'Available']
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
