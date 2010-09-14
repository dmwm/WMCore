from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getOverview, getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def getGlobalSummaryView():
    print "BCD"
    requestInfo = getOverview()
    #get information from global queue.
    gQueues = getGlobalQueues()
    gRequestInfo, cRequestInfo = _globalQueueInfo(gQueues)

    return _formatTable(requestInfo, gRequestInfo, cRequestInfo)


def _globalQueueInfo(gQueues):

    gRequestInfo = []
    cRequestInfo = []
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        #globalQ = WorkQueue({'endpoint': 'http://cmssrv75.fnal.gov:9991/workqueue%s' % '/'})
        gRequestInfo.extend(globalQ.getChildQueuesByRequest())
        cRequestInfo.extend(_localQueueInfo(globalQ))
        print cRequestInfo
    return gRequestInfo, cRequestInfo

def _localQueueInfo(globalQ):
    cQueues = globalQ.getChildQueues()
    jobSummary = []
    for cQueue in cQueues:
        childQ = WorkQueue({'endpoint': cQueue + "/"})
        #childQ = WorkQueue({'endpoint': 'http://cmssrv75.fnal.gov:9991/workqueue%s' % '/'})
        jobSummary.extend(childQ.getJobSummaryFromCouchDB())
    return jobSummary

def _formatTable(requestInfo, gRequestInfo, cRequestInfo):

    for item in requestInfo:
        for gItem in gRequestInfo:
            if item['request_name'] == gItem['request_name']:
                #TODO this should be handled correctly if ther will be multiple queues
                item.update(gItem)
        for cItem in cRequestInfo:
            if item['request_name'] == cItem['request_name']:
                #TODO this should be handled correctly if ther will be multiple queues
                item.update(cItem)

    return requestInfo