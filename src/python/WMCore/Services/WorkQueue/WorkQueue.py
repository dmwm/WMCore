import urllib
import logging
import os
import pwd

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker
    
class WorkQueue(Service):

    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, dict = {}):
        """
        """
        dict.setdefault('secure', False)
        if not dict.has_key('endpoint'):
            dict['endpoint'] = "%cmsweb.cern.ch/workqueue/" % \
                                ((dict['secure'] and "https://" or "http://"))

        dict.setdefault("accept_type", "application/json")
        dict.setdefault("content_type", "application/json")
        dict.setdefault('cacheduration', 0)

        self.encoder = JsonWrapper.dumps
        self.decoder = self.jsonThunkerDecoder
        
        Service.__init__(self, dict)
    
    def jsonThunkerDecoder(self, data):
        if data:
            thunker = JSONThunker()
            return thunker.unthunk(JsonWrapper.loads(data))
        else:
            return {}
        
    def _getResult(self, callname, clearCache = True,
                   args = None, verb="POST", contentType = None):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        # make base file name from call name.
        file = callname.replace("/", "_")
        if clearCache:
            self.clearCache(file, args, verb)

        # can't pass the decoder here since refreshCache wright to file
        f = self.refreshCache(file, callname, args, encoder = self.encoder,
                              verb = verb, contentType = contentType)
        result = f.read()
        f.close()
        result = self.decoder(result)

        return result
    
    def getWork(self, siteJobs, pullingQueueUrl = None, team = None):
        
        """
        _getWork_

        """
        args = {}
        args['siteJobs'] = siteJobs
        args['pullingQueueUrl'] = pullingQueueUrl
        args['team'] = team
        
        callname = 'getwork'
        return self._getResult(callname, args = args, verb = "POST")
    
    def status(self, status = None, before = None, after = None, 
               elementIDs = None, dictKey = None):
        args = []
        if status != None:
            args.append(('status', status))
        if before != None:
            args.append(('before', before))
        if after != None:
            args.append(('after', after))
        if dictKey != None:
            args.append(('dictKey', dictKey))
        if elementIDs != None:
            if type(elementIDs) != list:
                raise TypeError, "elementIDs should be list of ids"
            for elementID in elementIDs:
                args.append(('elementIDs', elementID))
        
        callname = 'status'
        return self._getResult(callname, args = args, verb = "GET",
                               contentType = "text/plain")
    
    def synchronize(self, child_url, child_report):
        """
        _synchronize_
        """
        args = {}
        args['child_report'] = child_report
        args['child_url'] = child_url
        
        callname = 'synchronize'
        return self._getResult(callname, args = args, verb = "PUT")
    
    def doneWork(self, elementIDs):
        """
        _doneWork_
        """
        args = {}
        args['elementIDs'] = elementIDs
        
        callname = 'donework'
        return self._getResult(callname, args = args, verb = "PUT")
    
    def failWork(self, elementIDs):
        """
        _failWork_
        """
        args = {}
        args['elementIDs'] = elementIDs
        
        callname = 'failwork'
        return self._getResult(callname, args = args, verb = "PUT")
    
    def cancelWork(self, elementIDs, id_type = "id"):
        """
        _cancelWork_

        id_type ignored, used for consistent signature with WorkQueue.cancelWork()
        """
        args = {}
        args['elementIDs'] = elementIDs
        args['id_type'] = id_type
        callname = 'cancelwork'

        return self._getResult(callname, args = args, verb = "PUT")

    def getChildQueues(self):
        """
        This service only provided by global queue
        """
        args = {}
        callname = 'childqueues'
        return self._getResult(callname, args = args, verb = "GET")

    def getChildQueuesByRequest(self):
        """
        This service only provided by global queue
        """
        args = {}
        callname = 'childqueuesbyrequest'
        return self._getResult(callname, args = args, verb = "GET")
    
    def getJobStatusByRequest(self):
        """
        This service only provided by global queue
        """
        args = {}
        callname = 'jobstatusbyrequest'
        return self._getResult(callname, args = args, verb = "GET")
    
    def getTopLevelJobsByRequest(self):
        """
        This service only provided by global queue
        """
        args = {}
        callname = 'jobsbyrequest'
        return self._getResult(callname, args = args, verb = "GET")

    def getJobSummaryFromCouchDB(self):
        """
        This service only provided by local queue
        """
        args = {}
        callname = 'jobsummary'
        return self._getResult(callname, args = args, verb = "GET")
    
    def getBatchJobStatus(self):
        """
        This service only provided by local queue
        """
        args = {}
        callname = 'batchjobstatus'
        return self._getResult(callname, args = args, verb = "GET")

    def getBatchJobStatusBySite(self):
        """
        This service only provided by local queue
        """
        args = {}
        callname = 'batchjobstatusbysite'
        return self._getResult(callname, args = args, verb = "GET")
    
    def queueWork(self, wmspecUrl, team, request):
        """
        This service only provided by local queue
        """
        if not (wmspecUrl or team or request):
            msg = "wmspecUrl, team, request should be specified"
            raise TypeError, msg

        args = {}
        args['wmspecUrl'] = wmspecUrl
        args['team'] = team
        args['request'] = request
        callname = 'queuework'
        return self._getResult(callname, args = args, verb = "PUT")

    def getSiteSummaryFromCouchDB(self):
        args = {}
        callname = 'jobstatebysite'
        return self._getResult(callname, args = args, verb = "GET")