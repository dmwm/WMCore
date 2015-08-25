'''
Created on Aug 13, 2014

@author: sryu
'''

from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.RequestDB.RequestDBReader import RequestDBReader
from WMCore.ReqMgr.CherryPyThreads.CherryPyPeriodicTask import CherryPyPeriodicTask

class CouchDBCleanup(CherryPyPeriodicTask):
    """
    CherryPy thread to clean up ACDC docs where the original request
    is in a final state (or announced).
    """
    def __init__(self, rest, config):

        CherryPyPeriodicTask.__init__(self, config)

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
        acdcService = CouchService(url=baseURL, database=acdcDB)
        originalRequests = acdcService.listCollectionNames()

        if len(originalRequests) == 0:
            return 

        deleteStates = ["rejected-archived", "aborted-archived", "normal-archived"]
        results = reqDB.getRequestByStatus(deleteStates) 

        # filter requests
        filteredRequests = list(set(originalRequests).intersection(results))

        total = 0
        for req in filteredRequests:
            try:
                deleted = acdcService.removeFilesetsByCollectionName(req)
                if deleted == None:
                    self.logger.warning("request already deleted %s" % req)
                else:
                    total += len(deleted)
                    self.logger.info("request %s deleted" % req)
            except:
                self.logger.error("fail to delete request: will try again %s" % req)
        self.logger.info("total %s requests deleted" % total)
        return
