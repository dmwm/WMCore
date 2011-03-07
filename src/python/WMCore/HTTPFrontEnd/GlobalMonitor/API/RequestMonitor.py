import logging

import WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter as DFormatter
from WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter import combineListOfDict
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def getRequestOverview(serviceURL, serviceLevel):

    if serviceLevel == "RequestManager":
        return getRequestInfoFromReqMgr(serviceURL)
    elif serviceLevel == "GlobalQueue":
        return getRequestInfoFromGlobalQueue(serviceURL)

    elif serviceLevel == "LocalQueue":
        return getRequestInfoFromLocalQueue(serviceURL)
    else:
        raise


def getRequestInfoFromReqMgr(serviceURL):
    """ get the request info from requestManager """

    service = RequestManager({'endpoint':serviceURL})
    try:
        baseResults = service.getRequestNames()
        print baseResults
        urls = service.getWorkQueue()
    except Exception, ex:
        logging.error(str(ex))
        return DFormatter.errorFormatter(serviceURL, "RequestManger Down")

    globalResults = []
    for url in urls:
        globalResults.extend(getRequestOverview(url, "GlobalQueue"))
    return combineListOfDict('request_name', baseResults, globalResults,
                             'global_queue')

def getRequestInfoFromGlobalQueue(serviceURL):
    """ get the request info from global queue """

    service = WorkQueue({'endpoint': serviceURL})
    try:
        jobInfo = service.getTopLevelJobsByRequest()
        qInfo = service.getChildQueuesByRequest()
    except Exception, ex:
        logging.error(str(ex))
        return DFormatter.errorFormatter(serviceURL, "GlobalQueue Down")
    else:
        baseResults = combineListOfDict('request_name', jobInfo, qInfo,
                                    local_queue = DFormatter.addToList)
        urls = service.getChildQueues()
        localResults = []
        for url in urls:
            localResults.append(getRequestOverview(url, "LocalQueue"))

        localQRules = {'pending': DFormatter.add, 'cooloff': DFormatter.add,
                       'running': DFormatter.add, 'success': DFormatter.add,
                       'failure': DFormatter.add,
                       'Pending': DFormatter.add, 'Running': DFormatter.add,
                       'Complete': DFormatter.add,'Error': DFormatter.add,
                       'inQueue': DFormatter.add, 'inWMBS': DFormatter.add
                       }
        return combineListOfDict('request_name', baseResults, localResults,
                                 'local_queue', **localQRules)

def getRequestInfoFromLocalQueue(serviceURL):
    """ get the request info from local queue """

    service = WorkQueue({'endpoint': serviceURL})
    try:
        jobStatusInfo = service.getJobStatusByRequest()
        batchJobs = service.getBatchJobStatus()
    except Exception, ex:
        logging.error(str(ex))
        return DFormatter.errorFormatter(serviceURL, "LocalQueue Down")

    try:
        couchJobs = service.getJobSummaryFromCouchDB()
    # this should be only CouchError, since localQueue error should be
    # caught above try except. doesn't try to catch CouchError to
    # reduce the  dependency (not to import CouchError)
    except Exception, ex:
        logging.error(str(ex))
        couchJobs = DFormatter.errorFormatter(serviceURL, "CouchDB Down")
    else:
        if len(couchJobs)  == 1 and couchJobs[0].has_key("error"):
            couchJobs = DFormatter.errorFormatter(serviceURL,
                                                  couchJobs[0]['error'])

    baseResults = combineListOfDict('request_name', jobStatusInfo, couchJobs)

    return combineListOfDict('request_name', baseResults, batchJobs)
