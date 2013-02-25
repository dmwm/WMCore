from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Wrappers import JsonWrapper as json
from WMCore.Lexicon import splitCouchServiceURL

# TODO: this could be derived from the Service class to use client side caching
class WorkQueue(object):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, couchURL, dbName = None):
        # if dbName not given assume we have to split
        if not dbName:
            couchURL, dbName = splitCouchServiceURL(couchURL)
        self.server = CouchServer(couchURL)
        self.db = self.server.connectDatabase(dbName, create = False)

    def getTopLevelJobsByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'jobsByRequest',
                                {'reduce' : True, 'group' : True})
        return [{'request_name' : x['key'],
                 'total_jobs' : x['value']} for x in data.get('rows', [])]

    def getChildQueues(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'childQueues',
                                {'reduce' : True, 'group' : True})
        return [x['key'] for x in data.get('rows', [])]

    def getChildQueuesByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'childQueuesByRequest',
                                {'reduce' : True, 'group' : True})
        return [{'request_name' : x['key'][0],
                 'local_queue' : x['key'][1]} for x in data.get('rows', [])]

    def getWMBSUrl(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrl',
                                {'reduce' : True, 'group' : True})
        return [x['key'] for x in data.get('rows', [])]

    def getWMBSUrlByRequest(self):
        """Get data items we have work in the queue for"""
        data = self.db.loadView('WorkQueue', 'wmbsUrlByRequest',
                                {'reduce' : True, 'group' : True})
        return [{'request_name' : x['key'][0],
                 'wmbs_url' : x['key'][1]} for x in data.get('rows', [])]

    def getJobStatusByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'jobStatusByRequest',
                                {'reduce' : True, 'group' : True})
        return [{'request_name' : x['key'][0], 'status': x['key'][1],
                 'jobs' : x['value']} for x in data.get('rows', [])]

    def getJobInjectStatusByRequest(self):
        """
        This service only provided by global queue
        """
        data = self.db.loadView('WorkQueue', 'jobInjectStatusByRequest',
                                {'reduce' : True, 'group' : True})
        return [{'request_name' : x['key'][0], x['key'][1]: x['value']}
                for x in data.get('rows', [])]

    def getAnalyticsData(self):
        """
        This getInject status and input dataset from workqueue
        """
        results = self.db.loadView('WorkQueue', 'jobInjectStatusByRequest',
                                {'reduce' : True, 'group' : True})
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
                                {'reduce' : True, 'group' : True})
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

    def cancelWorkflow(self, wf):
        """Cancel a workflow"""
        nonCancelableElements = ['Done', 'Canceled', 'Failed']
        data = self.db.loadView('WorkQueue', 'elementsDetailByWorkflowAndStatus',
                                {'startkey' : [wf], 'endkey' : [wf, {}],
                                 'reduce' : False})
        elements = [x['id'] for x in data.get('rows', []) if x['key'][1] not in nonCancelableElements]
        return self.updateElements(*elements, Status = 'CancelRequested')
