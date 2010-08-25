import urllib
import logging
import os
import pwd

from WMCore.Wrappers import jsonwrapper
from WMCore.Services.Service import Service

try:
    # Python 2.6
    import json
except ImportError:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class WorkQueue(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict = {}, responseType = "json", secure = False):
        """
        responseType will be either xml or json
        """
        self.responseType = responseType.lower()

        if not dict.has_key('endpoint'):
            dict['endpoint'] = "%cmsweb.cern.ch/workqueue/" % \
                                ((secure and "https://" or "http://"))
        if dict.has_key('cachepath'):
            pass
        elif os.getenv('WORKQUEUE_CACHE_DIR'):
            dict['cachepath'] = os.getenv('TIER0_MONITOR_CACHE_DIR') + '/.workqueue_cache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.workqueue_cache'
        else:
            dict['cachepath'] = '/tmp/.workqueue_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.makedirs(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = dict['cachepath'] + '/jsonparser.log',
                    filemode = 'w')
            dict['logger'] = logging.getLogger('WorkQueueParser')

        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
                   args = None, verb="POST"):
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
            self.clearCache(file, args)
        try:
            # overwrite original self['method']
            # this is only place used self['method'], it is safe to overwrite
            # If that changes keep the reset to original self['method']
            
            self["method"] = verb
            f = self.refreshCache(file, callname, args)
            result = f.read()
            f.close()

        except IOError, ex:
            raise RuntimeError("URL not available: %s" % callname)

#        if self.responseType == "json":
#            decoder = json.JSONDecoder()
#            return decoder.decode(result)

        return result
    
    def getWork(self, siteJobs, pullingQueueUrl=None):
        
        """
        _getWork_

        """
        args = siteJobs
        args['PullingQueueUrl'] = pullingQueueUrl
        
        callname = 'getwork'
        return self._getResult(callname, args = args, verb="POST")
    
    def synchronize(self, child_url, child_report):
        """
        _synchronize_
        """
        encodedChildReport = jsonwrapper.dumps(child_report)
        args = {}
        args['child_report'] = encodedChildReport
        args['child_url'] = child_url
        
        callname = 'synchronize'
        return self._getResult(callname, args = args, verb="PUT")
    
    def doneWork(self, elementIDs):
        """
        _doneWork_
        """
        encodedElementIDs = jsonwrapper.dumps(elementIDs)
        args = {}
        args['elementIDs'] = encodedElementIDs
        
        callname = 'donework'
        return self._getResult(callname, args = args, verb="PUT")
    
    def failWork(self, elementIDs):
        """
        _failWork_
        """
        encodedElementIDs = jsonwrapper.dumps(elementIDs)
        args = {}
        args['elementIDs'] = encodedElementIDs
        
        callname = 'failwork'
        return self._getResult(callname, args = args, verb="PUT")
    
    def cancelWork(self, elementIDs):
        """
        _cancelWork_
        """
        encodedElementIDs = jsonwrapper.dumps(elementIDs)
        args = {}
        args['elementIDs'] = encodedElementIDs
        
        callname = 'cancelwork'
        return self._getResult(callname, args = args, verb="PUT")
    
    def gotWork(self, elementIDs):
        """
        _gotWork_
        """
        encodedElementIDs = jsonwrapper.dumps(elementIDs)
        args = {}
        args['elementIDs'] = encodedElementIDs
        
        callname = 'gotwork'
        return self._getResult(callname, args = args, verb="PUT")
    