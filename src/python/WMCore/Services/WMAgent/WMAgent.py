import json

from WMCore.Services.Service import Service


class WMAgent(Service):

    """
    API for dealing with retrieving information from WMAgent RESTModel Service
    """

    def __init__(self, dict={}):
        """
        responseType will be JSON
        """

        dict.setdefault("accept_type", "application/json")
        dict.setdefault("content_type", "application/json")
        self.encoder = json.dumps
        self.decoder = json.loads

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
