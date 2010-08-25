import os
import pwd
import logging
from WMCore.Services.Service import Service
from WMCore.Services.AuthorisedService import AuthorisedService
# This should be deprecated in preference to simplejson once SiteDB spits out
# correct json
from WMCore.Services.JSONParser.JSONParser import JSONParser
#TODO: this should move to the AuthorisedService class
try:
    # Python 2.6
    import json
except ImportError:
    # Prior to 2.6 requires simplejson
    import simplejson as json
class WMBS(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict={}, responseType="xml"):
        """
        responseType will be either xml or json
        """
        self.responseType = responseType.lower()
        
        #if self.responseType == 'json':
            #self.parser = JSONParser()
        #elif self.responseType == 'xml':
            #self.parser = XMLParser()
            
        if os.getenv('WMBS_SERV_CACHE_DIR'):
            dict['cachepath'] = os.getenv('WMBS_SERV_CACHE_DIR') + '/.wmbs_service_cache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.wmbs_service_cache'
        else:
            dict['cachepath'] = '/tmp/wmbs_service_cache_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.mkdir(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=dict['cachepath'] + '/wmbs_service.log',
                    filemode='w')
            dict['logger'] = logging.getLogger('WMBSParser')
        
        #TODO if service doesn't need to be authorized, have switch to use Service
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
            print callname
            f = self.refreshCache(file, verb, callname, args)
            result = f.read()
            f.close()

        except IOError, ex:
            raise RuntimeError("URL not available: %s" % callname)

        if self.responseType == "json":
            decoder = json.JSONDecoder()
            return decoder.decode(result)

        return result

    def samplePOSTMethod(self, **args):

        """
        _samplePOSTMethod_

        """

        callname = 'samplePOSTMethod'
        return self._getResult(callname, args = args, verb="POST")

    def sampleGETMethod(self, **args):

        """
        _sampleGETMethod_

        """

        callname = 'sampleGETMethod'
        
        return self._getResult(callname, args = args, verb="GET")
    
    def samplePUTTMethod(self, **args):

        """
        _sampleGETMethod_

        """

        callname = 'samplePUTMethod'
        return self._getResult(callname, args = args, verb="PUT")

    def sampleDELETEMethod(self, **args):

        """
        _sampleGETMethod_

        """
        print "DETEEEEEEE %s" % args
        callname = 'sampleDELETEMethod'
        
        return self._getResult(callname, args = args, verb="DELETE")

