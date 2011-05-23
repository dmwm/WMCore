from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue

def getSiteOverview():
    """
    get summary view of workflow 
    getting information from global queue and localqueue
    LocalQueue endpoint and wmagent endpoint should use the same port
    """
    
    gQueues = getGlobalQueues()
    combinedResults = []
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        childQueues = globalQ.getChildQueues()
        for childQueue in childQueues:
            wqService = WorkQueue({'endpoint': childQueue})
            batchJobs = wqService.getBatchJobStatusBySite()
            completeJobs = wqService.getSiteSummaryFromCouchDB()
            _combineSites(combinedResults, batchJobs)
            _combineSites(combinedResults, completeJobs)
    return combinedResults

def _combineSites(results, batchJobs):

    for batchJob in batchJobs:
        newSite = True
        for item in results:
            if item['site_name'] == batchJob['site_name']:
                newSite = False
                for status in ['Pending', 'Running', 'Complete','Error',
                               'success', 'failure', 'cooloff']:
                    item.setdefault(status, 0)
                    batchJob.setdefault(status, 0)
                    item[status] += batchJob[status]
                batchJob.setdefault('job_slots', 0)
                item['job_slots'] += batchJob['job_slots']
        if newSite:
            results.append(batchJob)
