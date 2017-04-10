from __future__ import division, print_function

import json

from WMCore.Services.Service import Service
from WMCore.Cache.GenericDataCache import MemoryCacheStruct

class ReqMgrAux(Service):
    """
    API for dealing with retrieving information from RequestManager dataservice

    """

    def __init__(self, url, header=None):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        header = header or {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = json.dumps
        Service.__init__(self, httpDict)
        # This is only for the unittest: never set it true unless it is unittest
        self._noStale = False

    def _getResult(self, callname, clearCache=True, args=None, verb="GET",
                   encoder=json.loads, decoder=json.loads, contentType=None):
        """
        _getResult_
        """
        cfile = callname.replace("/", "_")
        if clearCache:
            self.clearCache(cfile, args, verb)

        f = self.refreshCache(cfile, callname, args, encoder=encoder,
                              verb=verb, contentType=contentType)
        result = f.read()
        f.close()

        if result and decoder:
            result = decoder(result)

        return result['result']

    def _getDataFromMemoryCache(self, callname):
        cache = MemoryCacheStruct(expire=0, func=self._getResult, initCacheValue={},
                                 kwargs={'callname': callname, "verb": "GET"})
        return cache.getData()

    def getCMSSWVersion(self):
        return self._getDataFromMemoryCache('cmsswversions')


    def populateCMSSWVersion(self, tc_url, **kwargs):
        from WMCore.Services.TagCollector.TagCollector import TagCollector
        cmsswVersions = TagCollector(tc_url, **kwargs).releases_by_architecture()

        return self["requests"].post('cmsswversions', cmsswVersions)[0]['result']

    def getWMAgentConfig(self):
        return self._getDataFromMemoryCache('wmagentconfig')
