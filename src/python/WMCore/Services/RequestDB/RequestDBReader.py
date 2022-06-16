from builtins import str, bytes, object
import time

from WMCore.Database.CMSCouch import CouchServer, Database
from WMCore.Lexicon import splitCouchServiceURL, sanitizeURL


class RequestDBReader(object):
    def __init__(self, couchURL, couchapp="ReqMgr"):
        # set the connection for local couchDB call
        self._commonInit(couchURL, couchapp)

    def _commonInit(self, couchURL, couchapp):
        """
        setting up comon variables for inherited class.
        inherited class should call this in their init function
        """
        if isinstance(couchURL, Database):
            self.couchDB = couchURL
            self.couchURL = self.couchDB['host']
            self.dbName = self.couchDB.name
            self.couchServer = CouchServer(self.couchURL)
        else:
            # NOTE: starting in CouchDB 3.x, we need to provide the couch credentials in
            # order to be able to write to the database, thus a RequestDBWriter object
            if isinstance(self.__class__, RequestDBReader):
                couchURL = sanitizeURL(couchURL)['url']
            self.couchURL, self.dbName = splitCouchServiceURL(couchURL)
            self.couchServer = CouchServer(self.couchURL)
            self.couchDB = self.couchServer.connectDatabase(self.dbName, False)
        self.couchapp = couchapp
        self.defaultStale = {"stale": "update_after"}

    def setDefaultStaleOptions(self, options):
        if not options:
            options = {}
        if 'stale' not in options:
            options.update(self.defaultStale)
        return options

    def _setNoStale(self):
        """
        Use this only for the unittest
        """
        self.defaultStale = {}

    def _getCouchView(self, view, options, keys=None):
        keys = keys or []
        options = self.setDefaultStaleOptions(options)

        if keys and isinstance(keys, (str, bytes)):
            keys = [keys]
        return self.couchDB.loadView(self.couchapp, view, options, keys)

    def _filterCouchInfo(self, couchInfo):
        # remove the couch specific information
        for key in ['_rev', '_attachments']:
            if key in couchInfo:
                del couchInfo[key]
        return

    def _formatCouchData(self, data, key="id", detail=True, filterCouch=True, returnDict=False):
        result = {}
        for row in data['rows']:
            if 'error' in row:
                continue
            if "doc" in row:
                if filterCouch:
                    self._filterCouchInfo(row["doc"])
                result[row[key]] = row["doc"]
            else:
                result[row[key]] = row["value"]
        if detail or returnDict:
            return result
        else:
            return list(result)

    def _getRequestByName(self, requestName, detail):
        """
        Retrieves a request dictionary from CouchDB
        :param requestName: string with the request name
        :param detail: boolean with False value for retrieving only the
            workflow name, or True for retrieving all its description
        :return: a list with the request name. Or a dictionary with the
            request description if detail=true
        """
        # this returns a dictionary with the workflow description, or
        # an empty dictionary if nothing is found in CouchDB
        result = self.couchDB.getDoc(requestName)
        if not result:
            return dict()
        if detail:
            result.pop('_attachments', None)
            result.pop('_rev', None)
            return {result['RequestName']: result}
        return [result['RequestName']]

    def _getRequestByNames(self, requestNames, detail):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail
        result = self.couchDB.allDocs(options, requestNames)
        return result

    def _getRequestByStatus(self, statusList, detail, limit, skip):
        """
        'status': list of the status
        """
        options = {}
        options["include_docs"] = detail

        if limit != None:
            options["limit"] = limit
        if skip != None:
            options["skip"] = skip
        keys = statusList
        return self._getCouchView("bystatus", options, keys)

    def _getRequestByStatusAndStartTime(self, status, detail, startTime):
        timeNow = int(time.time())
        options = {}
        options["include_docs"] = detail
        options["startkey"] = [status, startTime]
        options["endkey"] = [status, timeNow]
        options["descending"] = False

        return self._getCouchView("bystatusandtime", options)

    def _getRequestByStatusAndEndTime(self, status, detail, endTime):
        """
        'status': is the status of the workflow
        'endTime': unix timestamp for end time
        """
        options = {}
        options["include_docs"] = detail
        options["startkey"] = [status, 0]
        options["endkey"] = [status, endTime]
        options["descending"] = False

        return self._getCouchView("bystatusandtime", options)

    def _getRequestByTeamAndStatus(self, team, status, limit):
        """
        'status': is the status of the workflow
        'startTime': unix timestamp for start time
        """
        options = {}
        if limit:
            options["limit"] = limit
        if team and status:
            options["key"] = [team, status]
        elif team and not status:
            options["startkey"] = [team]
            options["endkey"] = [team, status]  # status = {}

        return self._getCouchView("byteamandstatus", options)

    def _getAllDocsByIDs(self, ids, include_docs=True):
        """
        keys is [id, ....]
        returns document
        """
        if len(ids) == 0:
            return []
        options = {}
        options["include_docs"] = include_docs
        result = self.couchDB.allDocs(options, ids)

        return result

    def getDBInstance(self):
        return self.couchDB

    def getRequestByNames(self, requestNames, detail=True):
        if len(requestNames) == 0:
            return {}
        if isinstance(requestNames, list) and len(requestNames) == 1:
            requestNames = requestNames[0]

        if isinstance(requestNames, (str, bytes)):
            requestInfo = self._getRequestByName(requestNames, detail=detail)
        else:
            requestInfo = self._getRequestByNames(requestNames, detail=detail)
            requestInfo = self._formatCouchData(requestInfo, detail=detail)
        return requestInfo

    def getRequestByStatus(self, statusList, detail=False, limit=None, skip=None):

        data = self._getRequestByStatus(statusList, detail, limit, skip)
        requestInfo = self._formatCouchData(data, detail=detail)

        return requestInfo

    def getRequestByStatusAndStartTime(self, status, detail=False, startTime=0):
        """
        Query for requests that are in a specific status since startTime.
        :param status: string with the workflow status
        :param detail: boolean flag used to return doc content or not
        :param startTime: unix start timestamp for your query
        :return: a list of request names
        """
        if startTime == 0:
            data = self._getRequestByStatus([status], detail, limit=None, skip=None)
        else:
            data = self._getRequestByStatusAndStartTime(status, detail, startTime)

        requestInfo = self._formatCouchData(data, detail=detail)
        return requestInfo

    def getRequestByStatusAndEndTime(self, status, detail=False, endTime=0):
        """
        Query for requests that are in a specific status until endTime.
        :param status: string with the workflow status
        :param detail: boolean flag used to return doc content or not
        :param endTime: unix end timestamp for your query
        :return: a list of request names
        """
        if endTime == 0:
            data = self._getRequestByStatus([status], detail, limit=None, skip=None)
        else:
            data = self._getRequestByStatusAndEndTime(status, detail, endTime)

        requestInfo = self._formatCouchData(data, detail=detail)
        return requestInfo

    def getRequestByTeamAndStatus(self, team, status, detail=False, limit=None):
        """
        'team': team name in which the workflow was assigned to.
        'status': a single status string.
        """
        if team and status:
            data = self._getRequestByTeamAndStatus(team, status, limit)
        elif team and not status:
            data = self._getRequestByTeamAndStatus(team, status={}, limit=limit)
        elif not team and not status:
            data = self._getRequestByTeamAndStatus(team={}, status={}, limit=limit)
        else:
            # nothing we can do with status only
            return

        requestInfo = self._formatCouchData(data, detail=detail)
        return requestInfo

    def getRequestByCouchView(self, view, options, keys=None, returnDict=True):
        keys = keys or []
        options.setdefault("include_docs", True)
        data = self._getCouchView(view, options, keys)
        requestInfo = self._formatCouchData(data, returnDict=returnDict)
        return requestInfo

    def getStatusAndTypeByRequest(self, requestNames):
        if isinstance(requestNames, (str, bytes)):
            requestNames = [requestNames]
        if len(requestNames) == 0:
            return {}
        data = self._getCouchView("byrequest", {}, requestNames)
        requestInfo = self._formatCouchData(data, returnDict=True)
        return requestInfo

    def getStepChainDatasetParentageByStatus(self, status):
        options = {}
        options["key"] = [False, status]
        data = self._getCouchView("byparentageflag", options)
        datasetParentageInfo = self._formatCouchData(data, returnDict=True)
        return datasetParentageInfo
