import urllib
import logging
import os
import pwd

from WMCore.Wrappers import JsonWrapper
from WMCore.Services.Service import Service

try:
    # Python 2.6
    import json
except ImportError:
    # Prior to 2.6 requires simplejson
    import simplejson as json

class RequestManager(Service):

    """
    API for dealing with retrieving information from RequestManager dataservice   
    
    """

    def __init__(self, dict = {}, secure = False):
        """
        responseType will be either xml or json
        """

        if not dict.has_key('endpoint'):
            #TODO needs to change proper default location
            dict['endpoint'] = "%scmssrv49.fnal.gov:8585/reqMgr/" % \
                                ((secure and "https://" or "http://"))
        if dict.has_key('cachepath'):
            pass
        elif os.getenv('REQUESTMGR_CACHE_DIR'):
            dict['cachepath'] = os.getenv('REQUESTMGR_CACHE_DIR') + '/.requestmgr_cache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.requestmgr_cache'
        else:
            dict['cachepath'] = '/tmp/.requestmgr_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.makedirs(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt = '%m-%d %H:%M',
                    filename = dict['cachepath'] + '/jsonparser.log',
                    filemode = 'w')
            dict['logger'] = logging.getLogger('RequestMgrParser')
            dict['accept_type'] = 'text/json'
        
        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
                   args = None, verb="GET"):
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

        result = JsonWrapper.loads(result)
        return result
    
    def getRequest(self, requestName=None):
        
        """
        _getRequest_

        """
        args = {}
        args['requestName'] = requestName
        
        callname = 'request'
        return self._getResult(callname, args = args, verb="GET")
    
    def getAssignment(self, teamName=None, request=None):
        
        args = {}
        args['teamName'] = teamName
        args['request'] = request
        
        callname = 'assignment'
        return self._getResult(callname, args = args, verb="GET")
    
    def postAssignment(self, requestName, prodAgentUrl=None):
        args = {}
        args['requestName'] = requestName
        
        callname = 'assignment'
        return self._getResult(callname, args = args, verb="POST")
    
# TODO: find the better way to handle emulation:
# hacky code: swap the namespace if emulator config is set 
from WMQuality.Emulators import emulatorSwitch
if emulatorSwitch("RequestManager"):
    from WMQuality.Emulators.RequestManagerClient.RequestManager import RequestManager

    