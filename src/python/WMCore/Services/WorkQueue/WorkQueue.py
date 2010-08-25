import urllib
import logging
import os
import pwd

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service
from WMCore.Wrappers.JsonWrapper.JSONThunker import JSONThunker

class WorkQueue():
    """
    API for dealing with retrieving information from WorkQueue DataService
    """

    def __init__(self, dict = None):
        """
        """
        self.params = dict or {}

        self.params.setdefault('secure', False)
        if not self.params.has_key('endpoint'):
            self.params['endpoint'] = "%cmsweb.cern.ch/workqueue/" % \
                                ((self.params['secure'] and "https://" or "http://"))
        if self.params.has_key('cachepath'):
            pass
        elif os.getenv('WORKQUEUE_CACHE_DIR'):
            self.params['cachepath'] = os.getenv('WORKQUEUE_CACHE_DIR') + '/.workqueue_cache'
        elif os.getenv('HOME'):
            self.params['cachepath'] = os.getenv('HOME') + '/.workqueue_cache'
        else:
            self.params['cachepath'] = '/tmp/.workqueue_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(self.params['cachepath']):
            os.makedirs(self.params['cachepath'])
        if 'logger' not in self.params.keys():
            logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = self.params['cachepath'] + '/jsonparser.log',
                    filemode = 'w')
            self.params['logger'] = logging.getLogger('WorkQueueParser')

        self.params.setdefault("accept_type", "application/json+thunker")
        self.params.setdefault("content_type", "application/json")

        self.encoder = JsonWrapper.dumps
        self.decoder = self.jsonThunkerDecoder

        # Need a seperate Service instance for each http method verb
        self.services = {}
        self.params['method'] = 'GET'
        self.services['GET'] = Service(self.params)
        self.params['method'] = 'PUT'
        self.services['PUT'] = Service(self.params)
        self.params['method'] = 'POST'
        self.services['POST'] = Service(self.params)

    def jsonThunkerDecoder(self, data):
        if data:
            thunker = JSONThunker()
            return thunker.unthunk(JsonWrapper.loads(data))
        else:
            return {}

    def _getResult(self, callname, clearCache = True,
                   args = None, verb = "POST"):
        """
        _getResult_

        retrieve JSON/XML formatted information given the service name and the
        argument dictionaries

        TODO: Probably want to move this up into Service
        """
        result = ''
        if args == None:
            argString = ''
        else:
            # rely on str() sorts dictionary by keys.
            #TODO: it is not guaranteed to generate unique hash for the different args
            argString = str(hash(str(args)))
        file = callname + argString + '.cache'
        if clearCache:
            self.services[verb].clearCache(file, args)

        # can't pass the decoder here since refreshCache wright to file
        #f = self.refreshCache(file, callname, args, encoder = self.encoder)
        f = self.services[verb].forceRefresh(file, callname, args, encoder = self.encoder)
        result = f.read()
        f.close()
        result = self.decoder(result)
        return result

    def getWork(self, siteJobs, pullingQueueUrl = None):

        """
        _getWork_

        """
        args = {}
        args['siteJobs'] = siteJobs
        args['pullingQueueUrl'] = pullingQueueUrl

        callname = 'getwork'
        return self._getResult(callname, args = args, verb = "POST")

    def status(self, status = None, before = None, after = None,
               elementIDs = None, dictKey = None):

        args = {}
        if status != None:
            args['status'] = status
        if before != None:
            args['before'] = before
        if after != None:
            args['after'] = after
        if dictKey != None:
            args['dictKey'] = dictKey
        if elementIDs != None:
            args['elementIDs'] = elementIDs

        callname = 'status'
        return self._getResult(callname, args = args, verb = "GET")

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

    def cancelWork(self, elementIDs):
        """
        _cancelWork_
        """
        args = {}
        args['elementIDs'] = elementIDs

        callname = 'cancelwork'
        return self._getResult(callname, args = args, verb = "PUT")

    def gotWork(self, elementIDs):
        """
        _gotWork_
        """
        args = {}
        args['elementIDs'] = elementIDs

        callname = 'gotwork'
        return self._getResult(callname, args = args, verb = "PUT")

