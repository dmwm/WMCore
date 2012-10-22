import logging
### use request manager funtion directly
### TODO: remove this when GlobalMonitor spins out as a separate application
try:
    from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
          import getOverview, getGlobalQueues
except:
    logging.warning("Not part of ReqMgr")
import WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter as DFormatter
from WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter import combineListOfDict
from WMCore.Lexicon import splitCouchServiceURL
from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMBS.WMBS import WMBS

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

    ###TODO: add back when GlobalMonitor spins out as a separate application
    service = RequestManager({'endpoint':serviceURL})
    try:
        ### use request manager funtion directly
        ### TODO: remove this when GlobalMonitor spins out as a separate application
        if serviceURL.lower() == "local":

            baseResults = getOverview()
            urls = getGlobalQueues()
        else:
            baseResults = service.getRequestNames()
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
    url, dbName = splitCouchServiceURL(serviceURL)
    service = WorkQueue(url, dbName)
    try:
        jobInfo = service.getTopLevelJobsByRequest()
        qInfo = service.getChildQueuesByRequest()
        siteWhitelists = service.getSiteWhitelistByRequest()
        childQueueURLs = set()
        for item in qInfo:
            childQueueURLs.add(item['local_queue'])

    except Exception, ex:
        logging.error("%s: %s" % (serviceURL, str(ex)))
        return DFormatter.errorFormatter(serviceURL, "GlobalQueue Down")
    else:
        tempResults = combineListOfDict('request_name', jobInfo, qInfo,
                                    local_queue = DFormatter.addToList)
        baseResults = combineListOfDict('request_name', tempResults,
                                        siteWhitelists)
        localResults = []
        for url in childQueueURLs:
            # TODO: change if each queue has shares the workflow
            # assume each queue has exclusive request
            localResults.extend(getRequestOverview(url, "LocalQueue"))

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

    url, dbName = splitCouchServiceURL(serviceURL)
    service = WorkQueue(url, dbName)
    try:
        wmbsUrls = service.getWMBSUrl()
        jobStatusInfo = service.getJobInjectStatusByRequest()
    except Exception, ex:
        logging.error("%s: %s" % (serviceURL, str(ex)))
        return DFormatter.errorFormatter(serviceURL, "LocalQueue Down")

    # assumes one to one relation between localqueue and wmbs
    if wmbsUrls:
        return getRequestInfoFromWMBS(wmbsUrls[0], jobStatusInfo)
    else:
        return []

def getRequestInfoFromWMBS(serviceURL, jobStatusInfo):

    service = WMBS({'endpoint':serviceURL})
    try:
        batchJobs = service.getBatchJobStatus()
    except Exception, ex:
        logging.error("%s: %s" % (serviceURL, str(ex)))
        return DFormatter.errorFormatter(serviceURL, "WMBS Service Dowtn")

    try:
        couchJobs = service.getJobSummaryFromCouchDB()
    # this should be only CouchError, since localQueue error should be
    # caught above try except. doesn't try to catch CouchError to
    # reduce the  dependency (not to import CouchError)
    except Exception, ex:
        logging.error("%s: %s" % (serviceURL, str(ex)))
        couchJobs = DFormatter.errorFormatter(serviceURL, "CouchDB Down")
    else:
        if len(couchJobs)  == 1 and couchJobs[0].has_key("error"):
            couchJobs = DFormatter.errorFormatter(serviceURL,
                                                  couchJobs[0]['error'])

    baseResults = combineListOfDict('request_name', jobStatusInfo, couchJobs)

    return combineListOfDict('request_name', baseResults, batchJobs)
