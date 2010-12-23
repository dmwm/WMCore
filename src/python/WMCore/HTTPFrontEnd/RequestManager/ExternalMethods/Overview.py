import logging
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getOverview, getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Database.CMSCouch import CouchError

def getGlobalSummaryView():
    """
    get summary view of workflow 
    getting information from global queue and localqueue
    """
    requestInfo = getOverview()
    #get information from global queue.
    gQueues = getGlobalQueues()
    gRequestInfo, cRequestInfo, qRequestInfo, bJobs = _globalQueueInfo(gQueues)

    return _formatTable(requestInfo, gRequestInfo, cRequestInfo, 
                        qRequestInfo, bJobs)


def _globalQueueInfo(gQueues):
    """
    getting information about workflow from global queue
    """
    gRequestInfo = []
    cRequestInfo = []
    qRequestInfo = []
    batchJobs = []
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        try:
            gRequestInfo.extend(globalQ.getChildQueuesByRequest())
            gRequestInfo.extend(globalQ.getTopLevelJobsByRequest())
        except Exception, ex:
            gRequestInfo.extend([{"queue_error": queue}])
            logging.error("GlobalQueue Error: %s" % str(ex))
        else:
            cJobs, qJobs, bJobs= _localQueueInfo(globalQ)
            cRequestInfo.extend(cJobs)
            qRequestInfo.extend(qJobs)
            batchJobs.extend(bJobs)
            
    return gRequestInfo, cRequestInfo, qRequestInfo, batchJobs

def _localQueueInfo(globalQ):
    """
    getting information from local queue
    """
    cQueues = globalQ.getChildQueues()
    jobSummary = []
    queueJobSummary = []    
    batchJobs = []
    for cQueue in cQueues:
        childQ = WorkQueue({'endpoint': cQueue + "/"})
        #childQ = WorkQueue({'endpoint': 'http://cmssrv75.fnal.gov:9991/workqueue%s' % '/'})
        try:
            jobData = childQ.getJobSummaryFromCouchDB()
        except CouchError, ce:
            jobSummary.extend([{"queue_error": cQueue, 
                                "error": ce.type}])
        except Exception, ex:
            jobSummary.extend([{"queue_error": cQueue,
                                "error": str(ex)}])
        else:
            if len(jobData)  == 1 and jobData[0].has_key("error"):
                jobSummary.extend([{"queue_error": cQueue,
                                    "couch_error": jobData[0]['error']}])
            else:
                jobSummary.extend(jobData)
                
        try:
            queueJobSummary.extend(childQ.getJobStatusByRequest())
        except Exception, ex:
            queueJobSummary.extend([{"queue_error": cQueue,
                                     "error": str(ex)}])
        try:
            batchJobs.extend(childQ.getBatchJobStatus())
        except Exception, ex:
            batchJobs.extend([{"queue_error": cQueue,
                               "error": str(ex)}])
         
    return jobSummary, queueJobSummary, batchJobs

def _formatTable(requestInfo, gRequestInfo, cRequestInfo, 
                 qRequestInfo, batchJobs):
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
        for gItem in gRequestInfo:

            if gItem.has_key('queue_error'):
                if item['global_queue'] == gItem['queue_error']:
                    item['error'] = "Global Queue Down"

            elif item['request_name'] == gItem['request_name']:
                if gItem.has_key('total_jobs'):
                    item.setdefault('total_jobs', 0)
                    item['total_jobs'] += gItem['total_jobs']
                else:
                    localQueueList = []
                    addToLocalQueueList(item, localQueueList)
                    addToLocalQueueList(gItem, localQueueList)
                    item.update(gItem)
                    item['local_queue'] = localQueueList

        for cItem in cRequestInfo:

            if cItem.has_key('queue_error'):
                if item.has_key('local_queue') and (
                                    cItem['queue_error'] in item.get('local_queue')):
                    item.setdefault('error', '')
                    item['error'] += "%s: %s" % (
                                cItem['queue_error'].strip('http://').strip('/workqueue'),
                                cItem.get('error', 'Down'))
                    if cItem.has_key('couch_error'):
                        item['couch_error'] = cItem['couch_error']

            elif item['request_name'] == cItem['request_name']:
                #Not just update items it should add the job numbers.
                #When there is multiple couchDB
                for status in ['pending', 'cooloff', 
                               'running', 'success', 'failure']:
                    jobs = item.pop(status, 0)
                    cJobs = cItem.pop(status, 0)
                    item[status] = jobs + cJobs

                item.update(cItem)
                
        for queueItem in qRequestInfo:

            if queueItem.has_key('queue_error'):
                # the error message will be the same as cItem error
                pass
            elif item['request_name'] == queueItem['request_name']:
                #
                for status in ['inQueue', 'inWMBS']:
                    jobs = item.pop(status, 0)
                    aJobs = queueItem.pop(status, 0)
                    item[status] = jobs + aJobs
        
        for batchJob in batchJobs:

            if batchJob.has_key('queue_error'):
                # the error message will be the same as cItem error
                pass
            elif item['request_name'] == batchJob['request_name']:
                #
                for status in ['Pending', 'Running', 'Complete','Error']:
                    jobs = item.pop(status, 0)
                    bJobs = batchJob.pop(status, 0)
                    item[status] = jobs + bJobs
    
    return requestInfo
