from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def getSiteOverview(serviceURL, serviceLevel):

    if serviceLevel == "RequestManager":
        return getSiteInfoFromReqMgr(serviceURL)
    elif serviceLevel == "GlobalQueue":
        return getSiteInfoFromGlobalQueue(serviceURL)
    elif serviceLevel == "LocalQueue":
        return getSiteInfoFromLocalQueue(serviceURL)
    else:
        raise

def getSiteInfoFromReqMgr(serviceURL):
    """ get agent info from request mgr """

    reqMgr = RequestManager({'endpoint':serviceURL})
    #get information from global queue.
    try:
        queues = reqMgr.getWorkQueue()
    except Exception, ex:
        errorInfo = {}
        errorInfo['site_name'] = serviceURL
        return [errorInfo]

    siteInfo = []
    for queueURL in queues:
        _combineSites(siteInfo, getSiteInfoFromGlobalQueue(queueURL))
    return siteInfo

def getSiteInfoFromGlobalQueue(serviceURL):

    globalQ = WorkQueue({'endpoint': serviceURL})
    try:
        queues = globalQ.getChildQueues()
    except Exception, ex:
        errorInfo = {}
        errorInfo['site_name'] = serviceURL
        return [errorInfo]

    siteInfo = []
    for queueURL in queues:
        _combineSites(siteInfo, getSiteInfoFromLocalQueue(queueURL))
    return siteInfo

def getSiteInfoFromLocalQueue(serviceURL):
    """ get agent status from local agent """

    wqService = WorkQueue({'endpoint': serviceURL})
    try:
        batchJobs = wqService.getBatchJobStatusBySite()
    except Exception, ex:
        return {}

    return batchJobs

def _combineSites(results, batchJobs):
    if results:
        return batchJobs
    for batchJob in batchJobs:
        newSite = True
        for item in results:
            if item['site_name'] == batchJob['site_name']:
                newSite = False
                for status in ['Pending', 'Running', 'Complete','Error']:
                    item.setdefault(status, 0)
                    batchJob.setdefault(status, 0)
                    item[status] += batchJob[status]
                item['job_slots'] += batchJob['job_slots']
        if newSite:
            results.append(batchJob)
