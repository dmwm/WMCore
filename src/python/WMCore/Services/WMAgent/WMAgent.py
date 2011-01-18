import os
import pwd
import logging
from WMCore.Services.Service import Service
from WMCore.Wrappers import JsonWrapper

class WMAgent(Service):

    """
    API for dealing with retrieving information from WMAgent RESTModel Service
    """

    def __init__(self, dict={}):
        """
        responseType will be either xml or json
        """

        #if self.responseType == 'json':
            #self.parser = JSONParser()
        #elif self.responseType == 'xml':
            #self.parser = XMLParser()

        if os.getenv('WMAGENT_SERV_CACHE_DIR'):
            dict['cachepath'] = os.getenv('WMAGENT_SERV_CACHE_DIR') + '/.wmagent_service_cache'
        elif os.getenv('HOME'):
            dict['cachepath'] = os.getenv('HOME') + '/.wmgent_service_cache'
        else:
            dict['cachepath'] = '/tmp/wmgent_service_cache_' + pwd.getpwuid(os.getuid())[0]
        if not os.path.isdir(dict['cachepath']):
            os.mkdir(dict['cachepath'])
        if 'logger' not in dict.keys():
            logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=dict['cachepath'] + '/wmagent_service.log',
                    filemode='w')
            dict['logger'] = logging.getLogger('WMAgentParser')

        dict.setdefault("accept_type", "application/json")
        dict.setdefault("content_type", "application/json")
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

    def getAgentStatus(self, detail = False):
        """
        """
        callname = 'agentstatus'
        # TODO support detail flag if it is needed
        # need to convert to boolean on serverside
        #args = {'detail': detail}
        args = {}
        return self._getResult(callname, args = args)

    def getACDCInfo(self):
        """
        """
        callname = 'acdclink'
        return self._getResult(callname)
