from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service

class ReqMgr(Service):

    """
    API for dealing with retrieving information from RequestManager dataservice

    """

    def __init__(self, url, header = {}):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = JsonWrapper.dumps
        Service.__init__(self, httpDict)
        # This is only for the unittest: never set it true unless it is unittest
        self._noStale = False

        
    def _getResult(self, callname, clearCache = True,
                   args = None, verb = "GET", encoder = JsonWrapper.loads, decoder = JsonWrapper.loads,
                   contentType = None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        file = callname.replace("/", "_")
        if clearCache:
            self.clearCache(file, args, verb)

        f = self.refreshCache(file, callname, args, encoder = encoder,
                              verb = verb, contentType = contentType)
        result = f.read()
        f.close()

        if result and decoder:
            result = decoder(result)
        return result
    
    def _createQuery(self, queryDict):
        """
        _createQuery
        queryDict is the format of 
        {'name': values} fair. value can be sting, int or list
        """
        if self._noStale:
            args = "_nostale=true&"
        else:
            args = ""
        for name, values in queryDict.items():
            if isinstance(values, basestring) or isinstance(values, int):
                values = [values]
            for val in values:
                args +='%s=%s&' % (name, val)
        
        return args.rstrip('&')

    def getRequestByNames(self, names):

        """
        _getRequest_

        """
        
        query = self._createQuery({'name': names})
        callname = 'request?%s' % query
        return self._getResult(callname, verb = "GET")['result']

    def getRequestByStatus(self, statusList, detail = False):
        """
        _getRequestByStatus_
        returns  list of requests
        [{'test_RequestString-OVERRIDE-ME_141125_142331_4966': {'BlockBlacklist': [],
                                                                    'BlockWhitelist': [],
                                                                    'CMSSWVersion': 'CMSSW_4_4_2_patch2',
                                                                    ....
                                                                    '_id': 'test_RequestString-OVERRIDE-ME_141125_142331_4966',
                                                                    'inputMode': 'couchDB'}}]
        """

        query = self._createQuery({'status': statusList})
        callname = 'request?%s' % query
        return self._getResult(callname, verb = "GET")['result']
    
    def insertRequests(self, requestDict):
        """
        _insertRequests_
        returns  list with following format
        [{'test_RequestString-OVERRIDE-ME_141125_142331_4966': {'BlockBlacklist': [],
                                                                    'BlockWhitelist': [],
                                                                    'CMSSWVersion': 'CMSSW_4_4_2_patch2',
                                                                    ....
                                                                    '_id': 'test_RequestString-OVERRIDE-ME_141125_142331_4966',
                                                                    'inputMode': 'couchDB'}}]
        need proper error handling if status is not 200 from orignal reporting.
        """
        return self["requests"].post('request', requestDict)[0]['result']

    def updateRequestStatus(self, request, status):
        """
        _updateRequestStatus_
        returns  list with following format
        [{'test_RequestString-OVERRIDE-ME_141125_142331_4966': {'BlockBlacklist': [],
                                                                    'BlockWhitelist': [],
                                                                    'CMSSWVersion': 'CMSSW_4_4_2_patch2',
                                                                    ....
                                                                    '_id': 'test_RequestString-OVERRIDE-ME_141125_142331_4966',
                                                                    'inputMode': 'couchDB'}}]
        need proper error handling if status is not 200 from orignal reporting.
        """
        
        status = {"RequestStatus": status}
        return self["requests"].put('request/%s' % request, status)[0]['result']

    def updateRequestProperty(self, request, propDict):
        """
        _updateRequestProperty_
        returns  list with following format
        [{'test_RequestString-OVERRIDE-ME_141125_142331_4966': {'BlockBlacklist': [],
                                                                    'BlockWhitelist': [],
                                                                    'CMSSWVersion': 'CMSSW_4_4_2_patch2',
                                                                    ....
                                                                    '_id': 'test_RequestString-OVERRIDE-ME_141125_142331_4966',
                                                                    'inputMode': 'couchDB'}}]
        need proper error handling if status is not 200 from orignal reporting.
        ({'result': data}, status_code, response, cache_flag)                                                            
        """
        return self["requests"].put('request/%s' % request, propDict)[0]['result']
    
        
