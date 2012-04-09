from WMCore.Database.CMSCouch import CouchServer, CouchNotFoundError
from WMCore.Wrappers import JsonWrapper as json

def splitCouchServiceURL(serviceURL):
    """
    split service URL to couchURL and couchdb name
    serviceURL should be couchURL/dbname format.
    """

    splitedURL = serviceURL.rstrip('/').rsplit('/', 1)
    #TODO: this is a hack needs to be removed.
    if (serviceURL.find("https://cmsweb.cern.ch/workqueue") != -1 or
        serviceURL.find("https://cmsweb-testbed.cern.ch/workqueue") != -1 or
        serviceURL.find("https://cmsweb-dev.cern.ch/workqueue") != -1):
        return "%s/couchdb" % splitedURL[0], splitedURL[1]
    else:
        return splitedURL[0], splitedURL[1]

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
        import urllib
        uri = "/" + self.db.name + "/_design/WorkQueue/_update/in-place/"
        data = {"updates" : json.dumps(updatedParams)}
        for ele in elementIds:
            thisuri = uri + ele + "?" + urllib.urlencode(data)
            answer = self.db.makeRequest(uri = thisuri, type = 'PUT')
        return

    def cancelWorkflow(self, wf):
        """Cancel a workflow"""
        data = self.db.loadView('WorkQueue', 'elementsByWorkflow', {'key' : wf})
        elements = [x['id'] for x in data.get('rows', [])]
        return self.updateElements(*elements, Status = 'CancelRequested')