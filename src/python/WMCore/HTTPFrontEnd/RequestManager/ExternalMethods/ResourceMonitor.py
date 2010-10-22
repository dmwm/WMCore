from collections import defaultdict
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMBS.WMBS import WMBS

def getResourceOverview():
    """
    get summary view of workflow 
    getting information from global queue and localqueue
    """
    
    gQueues = getGlobalQueues()
    resourceInfo  = defaultdict(dict)
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        childQueues = globalQ.getChildQueues()
        for childQueue in childQueues:
            wmbsService = WMBS({'endpoint': 
                    childQueue.replace('/workqueue', '/wmbs')})
            resourceDict = wmbsService.getResourceInfo(tableFormat = False)
            for site, data in resourceDict.items():
                for jobState, jobNum in data.items():
                    resourceInfo[site].setdefault(jobState, 0)
                    resourceInfo[site][jobState] += jobNum
    
    
    return _formatTable(resourceInfo)

def _formatTable(formattedDict):
    """
    _formatTable_
    """
    results = []
    for k, v in formattedDict.items():
        item = {}
        item['site'] = k
        item.update(v)
        results.append(item)
    return results
