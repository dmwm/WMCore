'''
Created on Aug 13, 2014

@author: sryu
'''
from __future__ import (division, print_function)

from WMCore.REST.CherryPyPeriodicTask import CherryPyPeriodicTask
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader

class CouchDBCleanup(CherryPyPeriodicTask):

    def __init__(self, rest, config):

        super(CouchDBCleanup, self).__init__(config)

    def setConcurrentTasks(self, config):
        """
        sets the list of functions which
        """
        self.concurrentTasks = [{'func': self.acdcCleanup, 'duration': config.acdcCleanDuration}]

    def acdcCleanup(self, config):
        """
        gather active data statistics
        """
        
        reqDB = RequestDBReader(config.reqmgrdb_url)

        from WMCore.ACDC.CouchService import CouchService
        baseURL, acdcDB = splitCouchServiceURL(config.acdc_url)
        acdcService = CouchService(url = baseURL, database = acdcDB)
        originalRequests = acdcService.listCollectionNames()
        
        if len(originalRequests) == 0:
            return 
        # filter requests
        results = reqDB._getCouchView("byrequest", {}, originalRequests)
        # checkt he status of the requests [announced, rejected-archived, aborted-archived, normal-archived]
        deleteStates = ["announced", "rejected-archived", "aborted-archived", "normal-archived"]
        filteredRequests = []
        for row in results["rows"]:
            if row["value"][0] in deleteStates:
                filteredRequests.append(row["key"])
                
        total = 0
        for req in filteredRequests:
            try:
                deleted = acdcService.removeFilesetsByCollectionName(req)
                if deleted == None:
                    self.logger.warning("request alread deleted %s", req)
                else:
                    total += len(deleted)
                    self.logger.info("request %s deleted", req)
            except Exception as ex:
                self.logger.error("request deleted failed: will try again %s: %s", req, str(ex))
        self.logger.info("total %s requests deleted", total)        
        return