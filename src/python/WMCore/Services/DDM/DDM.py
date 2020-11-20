from __future__ import (division, print_function)

import json
import logging

from WMCore.Services.Service import Service
from pprint import pformat


class DDMReqTemplate(dict):
    """
    A simple class defining a set of request templates for DDM, with default
    structures representing the different POST calls expected by the modules
    here:
        https://github.com/SmartDataProjects/dynamo/tree/master/lib/web/modules
    """

    def __init__(self, api, **kwargs):
        """
        api:
            String representing the type of request to be sent. In case the api
            is missing or unrecognized the created object will be an ordinary
            dictionary.
        example:
            ddmReq = DDMReqTemplate('copy',
                                     item=['/LQLQToTop..._M-1200/NANOAODSIM'],
                                     site=['T2_CH_CERN', 'T2_US_MIT'])
        NOTE: we do not enforce the use of api parameter, but we may change this
        """
        super(DDMReqTemplate, self).__init__(**kwargs)

        self.api = api

        # tempTuple format: [(key, defaultValue, type), ....()]
        # :item: list of datasets
        # :site: list of destination sites
        # :n: Number of copies
        # :cache: cache the request on the DDM side for quicker copypoll calls
        # :request_id: The Request id assigned by DDM
        if api == 'copy':
            tempTuple = [('item', [], list),
                         ('site', ['T2_*', 'T1_*_Disk'], list),
                         ('group', 'DataOps', str),
                         ('n', None, int),
                         ('cache', None, str)]
            required = ['item', 'site']

        elif api == 'pollcopy':
            tempTuple = [('request_id', None, int),
                         ('item', None, list),
                         ('site', None, list),
                         ('status', None, str),
                         ('user', None, str)]
            required = ['request_id']

        elif api == 'cancelcopy':
            tempTuple = [('request_id', None, int)]
            required = ['request_id']

        else:
            msg = "ERROR: Unsupported API: {}".format(api)
            raise ValueError(msg)

        # buildTemplate:
        template = dict()
        for tup in tempTuple:
            template.update({tup[0]: tup[1]})

        # check if we can fit all the arguments provided through **kwargs
        for kw, arg in kwargs.iteritems():
            found = False
            typeok = False
            for tup in tempTuple:
                if kw == tup[0]:
                    found = True
                    # pass the type check for None values but do not substitute the
                    # default value with None - use the default provided from the template
                    if kwargs[kw] is None:
                        typeok = True
                    # Perform a type check and if passed absorb the value from kwargs
                    elif isinstance(kwargs[kw], tup[2]):
                        typeok = True
                        template[kw] = kwargs[kw]
            if not found:
                msg = "ERROR: Unrecognized parameter: {}: {}".format(kw, kwargs[kw])
                raise KeyError(msg)
            if not typeok:
                msg = "ERROR: Wrong type: {} for parameter: {}: {}".format(
                    type(kwargs[kw]),
                    kw,
                    kwargs[kw])
                raise TypeError(msg)

        # check if all mandatory fields do exist
        valid = []
        missing = []
        for mandField in required:
            if mandField in template.keys() and template[mandField]:
                valid.append(True)
            else:
                valid.append(False)
                missing.append(mandField)

        if not all(valid):
            msg = "ERROR: Missing Mandatory Fields: {} for: {}".format(missing, template)
            raise KeyError(msg)

        self.update(template)

    def strip(self):
        """
        _strip_

        Method used to drop all the fields with value None. DDM does not
        handle those quite well, and they better be removed from the request.
        """

        for key in list(self):
            if self[key] is None:
                self.pop(key)

    def isEqual(self, ddmReq, exclude=None):
        """
        _isEqual_

        Equivalence method.
        :exclude:
            A string representing the key to be excluded from the comparison.
        :return:
            Bool
        """
        # first we check the apis - we cannot compare apples to oranges:
        if self.api != ddmReq.api:
            return False

        # in case no key have been excluded it is a normal dict comparison
        if not exclude:
            return self == ddmReq

        # The final check: we iterate over the keys of the two objects in order
        # to be sure we do not leave any extra key which is present in one of
        # the objects but not in the other
        allKeys = set(self.keys()) | set(ddmReq.keys())
        for key in allKeys:
            if key == exclude:
                continue
            try:
                if ddmReq[key] != self[key]:
                    return False
            except KeyError:
                return False
        return True


class DDM(Service):
    """
    Class which provides client APIs to the DDM service.

    Usage example:
        api = 'copy'
        ddmReq = DDMReqTemplate(api)
        ddm = DDM()
        ddm.makeRequest(ddmReq, api)
    """

    def __init__(self,
                 url=None,
                 logger=None,
                 configDict=None,
                 enableDataPlacement=True):
        """
        configDict:
            Dictionary with parameters that are passed to the super class.
        enableDataPlacement:
            If `False` just create the request templates without sending them.
        """
        url = url or "https://dynamo.mit.edu"
        configDict = configDict or {}
        configDict.setdefault('endpoint', url)
        configDict.setdefault('cacheduration', 1)  # in hours
        configDict.setdefault('accept_type', 'application/json')
        configDict.setdefault('content_type', 'application/json')
        configDict['logger'] = logger if logger else logging.getLogger()
        super(DDM, self).__init__(configDict)
        self.enableDataPlacement = enableDataPlacement
        self['logger'].debug("Initializing DDM with url: %s", self['endpoint'])

    def _getResult(self, ddmReq, apiUrl, callname):
        """
        Either fetch data from the cache file or query the data-service
        :param metricNumber: a number corresponding to the SSB metric
        :return: a dictionary

        """

        # TODO: to make this cache not per call but per DDM object instance
        cachedApi = "%s.json" % callname

        # striping the None values from the request before sending it to dynamo
        ddmReq.strip()
        msg = "INFO: sending data to DDM: "
        msg += "API: {}: ddmReq: {}".format(ddmReq.api, pformat(ddmReq))
        self['logger'].debug(msg)

        # The Request class expects this to be a dict, otherwise it tries to
        # encode the inputdata once again and inserts parasite information and
        # breaks the call to dynamo.
        ddmReq = dict(ddmReq)
        data = self.refreshCache(cachedApi,
                                 apiUrl,
                                 inputdata=ddmReq,
                                 verb='POST')
        results = data.read()
        data.close()

        results = json.loads(results)
        return results

    def makeRequest(self, ddmReq):
        """
        _makeRequest_

        Makes a SINGLE request to DDM

        :ddmReq: Request template an instance of DDMReqTemplate class.
        :return: Dictionary containing the result from DDM
        """

        # try to figure out the api from the ddmReq template
        api = ddmReq.api

        if not api:
            msg = "WARNING: skip sending data to DDM: "
            msg += "API: {}: ddmReq: {}\n".format(ddmReq.api, pformat(ddmReq))
            return None

        apiUrl = "registry/request/{}".format(api)
        callname = "DDM-{}".format(api)

        if not self.enableDataPlacement:
            return ddmReq

        try:
            # TODO: here/getResult to send the request parameters to the logger
            result = self._getResult(ddmReq, apiUrl, callname)
        except Exception as exc:
            msg = "Failed to make a DDM request for data dictionary: {}".format(ddmReq)
            msg += " Error details: {}".format(str(exc))
            self['logger'].error(msg)
            return None
        return result

    def makeAggRequests(self, ddmReqList, aggKey=None):
        """
        _makeAggRequest_

        Makes several Aggregated requests to DDM. Takes the list of objects,
        instances of ddmReqTemplate and aggregates it based on a selected key.
        It iterates through the list and compares all to all. Uses the
        ddmReqTemplate internal Equivalence method for comparison with the
        aggregation key excluded. If a match is found the two requests are
        squeezed into one. The final result should be a new list of unique
        ddmRequests each one of them should contain all similar ddmRequests
        from the initial list with the values of the aggregation key accumulated
        in a list under the same key name. Creates a request per ddmReqTemplate
        from the list with aggregated templates.
        NOTE:
            In this method the api is always fetched from the template.
        TODO:
            So far it is working basically for the 'copy' API and due to that
            the value of the aggregation key has to be a dictionary so no
            explicit check needs to be done for the type. In case we decide to
            use it we should develop this to the end with the proper type check
            for that key

        :ddmReqList: List of objects, instances of DDMReqTemplate class.
        :aggKey:    String representing the Aggregation key.
        :return:     List of Dictionaries containing the results from DDM

        """
        if not ddmReqList or not isinstance(ddmReqList, list):
            return None

        ddmReqListAgg = []

        # populate the first element in the aggregated list
        ddmReqListAgg.append(ddmReqList.pop())

        # feed the rest
        while len(ddmReqList) != 0:
            ddmReq = ddmReqList.pop()
            found = False
            for aggReq in ddmReqListAgg:
                if ddmReq.isEqual(aggReq, aggKey):
                    # Check if the two objects are not references to one and the
                    # same object. Only then copy the values of the aggKey,
                    # otherwise we will enter an endless cycle.
                    if ddmReq is not aggReq and aggKey is not None:
                        for i in ddmReq[aggKey]:
                            aggReq[aggKey].append(i)
                    found = True
                    del(ddmReq)
                    break
            if not found:
                ddmReqListAgg.append(ddmReq)

        if not self.enableDataPlacement:
            return ddmReqListAgg

        results = []
        for ddmReq in ddmReqListAgg:
            api = ddmReq.api
            if not api:
                msg = "WARNING: skip sending data to DDM: "
                msg += "API: {}: ddmReq: {}\n".format(ddmReq.api, pformat(ddmReq))
                self['logger'].debug(msg)
                results.append(None)
                continue

            # here to be noticed - we create a cache file per request
            apiUrl = "registry/request/{}".format(api)
            callname = "DDM-{}".format(api)
            try:
                result = self._getResult(ddmReq, apiUrl, callname)
                results.append(result)
            except Exception as e:
                msg = "ERROR: sending data to DDM: "
                msg += "API: {}: ddmReq: {}\n".format(ddmReq.api, pformat(ddmReq))
                msg += "ERROR: {}".format(str(e))
                self['logger'].error(msg)
                print(str(e))
                results.append(None)
        return results
