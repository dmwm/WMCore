import os
import pwd
import logging
from WMCore.Services.Service import Service
from WMCore.Wrappers import JsonWrapper

class WMBS(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict={}):
        """
        responseType will be either xml or json
        """
        
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
        
        self.encoder = JsonWrapper.dumps
        self.decoder = JsonWrapper.loads
        
        Service.__init__(self, dict)

    def _getResult(self, callname, clearCache = True,
                   args = None, verb="GET", contentType = None):
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
    
    def getResourceInfo(self, tableFormat = True):
        """
        """
        callname = 'listthresholdsforcreate'
        args = {'tableFormat': tableFormat}
        return self._getResult(callname, args = args)