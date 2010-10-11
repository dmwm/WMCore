from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getOverview, getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def getGlobalSummaryView(host):
    """
    get summary view of workflow 
    getting information from global queue and localqueue
    """
    requestInfo = getOverview()
    #get information from global queue.
    gQueues = getGlobalQueues()
    gRequestInfo, cRequestInfo = _globalQueueInfo(gQueues)

    return _formatTable(requestInfo, gRequestInfo, cRequestInfo, host)


def _globalQueueInfo(gQueues):
    """
    getting information about workflow from global queue
    """
    gRequestInfo = []
    cRequestInfo = []
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        try:
            gRequestInfo.extend(globalQ.getChildQueuesByRequest())
        except:
            gRequestInfo.extend([{"queue_error": queue}])
        else:
            cRequestInfo.extend(_localQueueInfo(globalQ))

    return gRequestInfo, cRequestInfo

def _localQueueInfo(globalQ):
    """
    getting information from local queue
    """
    cQueues = globalQ.getChildQueues()
    jobSummary = []
    for cQueue in cQueues:
        childQ = WorkQueue({'endpoint': cQueue + "/"})
        #childQ = WorkQueue({'endpoint': 'http://cmssrv75.fnal.gov:9991/workqueue%s' % '/'})
        try:
            jobSummary.extend(childQ.getJobSummaryFromCouchDB())
        except:
            #pass
            jobSummary.extend([{"queue_error": cQueue}])

    return jobSummary

def _formatTable(requestInfo, gRequestInfo, cRequestInfo, host):
    """
    combine the results from different sources and format them
    """
    def addToLocalQueueList(dictItem, queueList):
        queue = dictItem.pop('local_queue', None)
        if queue:
            if type(queue) == list:
                queueList.extend(queue)
            else:
                queueList.append(queue)
        return queueList
    
        
    for item in requestInfo:
        item['host'] = host
        for gItem in gRequestInfo:

            if gItem.has_key('queue_error'):
                if item['global_queue'] == gItem['queue_error']:
                    item['error'] = "Global Queue Down"

            elif item['request_name'] == gItem['request_name']:
                localQueueList = []
                addToLocalQueueList(item, localQueueList)
                addToLocalQueueList(gItem, localQueueList)
                item.update(gItem)
                item['local_queue'] = localQueueList

        for cItem in cRequestInfo:

            if cItem.has_key('queue_error'):
                if item.has_key('local_queue') and (cItem['queue_error'] in item.get('local_queue')):
                    item.setdefault('error', 'Local Queue Down')
                    item['error'] += ", %s" % cItem['queue_error'].strip('http://').strip('/workqueue')

            elif item['request_name'] == cItem['request_name']:
                #Not just update items it should add the job numbers.
                #When there is multiple couchDB
                for status in ['pending', 'cooloff', 
                               'running', 'success', 'failure']:
                    jobs = item.pop(status, 0)
                    cJobs = cItem.pop(status, 0)
                    item[status] = jobs + cJobs

                item.update(cItem)


    return requestInfo