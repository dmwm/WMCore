import json
import logging

from WMCore.Services.Service import Service


class WMStatsServer(Service):
    """
    API to query wmstats server: mostly against acive datacache

    """

    def __init__(self, url, header=None, logger=None):
        """
        responseType will be either xml or json
        """

        httpDict = {}
        header = header or {}
        # url is end point
        httpDict['endpoint'] = "%s/data" % url
        httpDict['logger'] = logger if logger else logging.getLogger()

        # cherrypy converts request.body to params when content type is set
        # application/x-www-form-urlencodeds
        httpDict.setdefault("content_type", 'application/json')
        httpDict.setdefault('cacheduration', 0)
        httpDict.setdefault("accept_type", "application/json")
        httpDict.update(header)
        self.encoder = json.dumps
        Service.__init__(self, httpDict)

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

    def _createQuery(self, queryDict):
        """
        _createQuery
        :param queryDict: is the format of {name: values} fair. value can be sting, int or list
        :type queryDict: dict
        :returns: url query string
        """
        args = ""
        for name, values in queryDict.items():
            if isinstance(values, (basestring, int)):
                values = [values]
            for val in values:
                args += '%s=%s&' % (name, val)

        return args.rstrip('&')

    def getActiveData(self):

        """
        _getActiveData_

        :returns:
        """

        callname = 'requestcache'
        return self._getResult(callname, verb="GET")[0]

    def getFilteredActiveData(self, inputCondition, outputMask):
        """
        _getFilteredActiveData_

        :param inputCondition: dict of Condition
        :type inputCondition: dict
        :param outputMask: list of output mask
        :type outputMask: list
        :returns:  list of dict or which passes the input condition and only result on outputMask
        """
        inputCondition.update({'mask':outputMask})
        query = self._createQuery(inputCondition)
        callname = 'filtered_requests?%s' % query
        return self._getResult(callname, verb="GET")

    def getChildParentDatasetMap(self, requestType="StepChain", parentageResolved=False):
        """

        :param requestType: specify the type of requests you want find the parentag
        :param dataset: child dataset
        :param includeInputDataset:
        :return: dict of {child_dataset_name: parent_dataset_name}
        """
        filter = {"RequestType": requestType, "ParentageResolved": parentageResolved}
        mask = ["ChainParentageMap"]

        results = self.getFilteredActiveData(filter, mask)
        childParentMap = {}
        for info in results:
            if info["ChainParentageMap"]:
                for childParentDS in info["ChainParentageMap"]:
                    if childParentDS["ParentDset"]:
                        for childDS in childParentDS["ChildDsets"]:
                            childParentMap[childDS] = childParentDS["ParentDset"]
        return childParentMap
