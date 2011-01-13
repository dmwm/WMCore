from collections import defaultdict
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
      import getGlobalQueues

from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMAgent.WMAgent import WMAgent

def getAgentOverview():
    """
    get summary view of workflow
    getting information from global queue and localqueue
    LocalQueue endpoint and wmagent endpoint should use the same port
    """

    gQueues = getGlobalQueues()
    agents = []
    for queue in gQueues:
        globalQ = WorkQueue({'endpoint': queue + "/"})
        childQueues = globalQ.getChildQueues()
        agentInfo = {}
        for childQueue in childQueues:
            agentUrl = childQueue.replace('/workqueue', '/wmagent')
            agentService = WMAgent({'endpoint': agentUrl})
            agent = agentService.getAgentStatus(detail = False)
            agentInfo['url'] = agentUrl
            agentInfo['status'] = agent['status']
            agentInfo['acdc'] = agentService.getACDCInfo()['url']
            agents.append(agentInfo)
    return agents
