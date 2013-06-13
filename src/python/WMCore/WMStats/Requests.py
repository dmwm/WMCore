"""
ReqMgr request handling.

"""

import time
import cherrypy
from datetime import datetime, timedelta

from WMCore.REST.Server import RESTEntity, restcall, rows
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import validate_str


class Requests(RESTEntity):
    
    def __init__(self, app, api, config, mount, db_handler):
        # main CouchDB database where requests/workloads are stored
        self.db_name = config.couch_reqmgr_db
        self.db_handler = db_handler
        
    def validate(self, apiobj, method, api, param, safe):
        if method in ['GET', 'PUT']:
            validate_strlist("requestNames", param, safe, rx.RX_REQUEST_NAME, optional=True)
            validate_str("all", param, safe, rx.RX_BOOL_FLAG, optional=True)
        
    
    @restcall
    def get(self, jobInfo=False, **kwargs):
        """
        Returns request info depending on the conditions set by kwargs
        Currently defined kwargs are following.
        statusList, requestNames, requestType, prepID, inputDataset, outputDataset, dateRange
        If jobInfo is True, returns jobInfomation about the request as well.
        """
        permittedParams = ["statusList", "names", "type", "prepID", "inputDataset", "outputDataset", "dateRange"]
        
        if statusList:
            requestInfo = self._getRequestsByStatus(statusList)
        if names:
            requestInfo = self._getReuestsByNames(names)
        if prepID:
            requestInfo = self._getRequestsByPrepID(prepID)
        if inputDataset:
            requestInfo = self._getRequestsByInputDataset(inputDataset)
        if outputDataset:
            requestInfo = self._getRequestsByOutputDataset(outputDataset)
        if dateRange:
            requestInfo = self._getRequestsByDateRange(dateRange)
        if jobInfo:
            requestWithJobInfo = self._getJobInfo(requestInfo)
    
    def _getRequestsByStatus(self, statusList):
        """
        TODO: need to have some restriction when status list contains archived requests
        """
        
        return requestInfo
    
    def _getReuestsByNames(self, names):
        """
        TODO: names can be regular expression or list of names
        """
        
        return requestInfo
    
    def _getRequestsByPrepID(self, prepID):
        """
        """
        return requestInfo
    
    def _getRequestsByInputDataset(self, inputDataset):
        """
        """
        return requestInfo
    
    def _getRequestsByOutputDataset(self, outputDataset):
        """
        """
        return requestInfo
    
    def _getRequestsByDateRange(self, dateRange):
        """
        """
        return requestInfo
    
    def _getJobInfo(self, requestInfo):
        """
        all ways use server cache
        """
        return 
    