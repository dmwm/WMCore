import json

from WMCore.Services.Service import Service


class WMBS(Service):

    """
    API for dealing with retrieving information from PhEDEx DataService
    """

    def __init__(self, dict={}):
        """
        responseType will be either xml or json
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

    def getJobSummaryFromCouchDB(self):
        """
        get the job status summary by request (workflow) from couchDB
        """
        args = {}
        callname = 'jobsummary'
        return self._getResult(callname, args = args, verb = "GET")

    def getSiteSummaryFromCouchDB(self):
        """
        returns number of complete jobs from the sites recorded in couchDB
        within the one hour period of calling. This might not the exact number
        since it could be gotten from the staled cache. (for performance reason)
        However, it should be close enough monitor sites' health.
        {'site': 'FNAL', 'complete': 1, 'success': 100, 'jobfailed':2}
        complete : job is completed but not accounted.
        """
        args = {}
        callname = 'jobstatebysite'
        return self._getResult(callname, args = args, verb = "GET")

    def getBatchJobStatus(self):
        """
        get the normalized job status from batch system
        """
        args = {}
        callname = 'batchjobstatus'
        return self._getResult(callname, args = args, verb = "GET")

    def getBatchJobStatusBySite(self):
        """
        get the normalized job status from batch system by sites
        """
        args = {}
        callname = 'batchjobstatusbysite'
        return self._getResult(callname, args = args, verb = "GET")

    def getResourceInfo(self, tableFormat = True):
        """
        """
        callname = 'listthresholdsforcreate'
        args = {'tableFormat': tableFormat}
        return self._getResult(callname, args = args)

    def getSiteList(self):
        """
        """
        callname = 'listsites'
        return self._getResult(callname)
