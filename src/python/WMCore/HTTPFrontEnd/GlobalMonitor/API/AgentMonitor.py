from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMAgent.WMAgent import WMAgent

def getAgentOverview(serviceURL, serviceLevel):

    if serviceLevel == "RequestManager":
        return getAgentInfoFromReqMgr(serviceURL)
    elif serviceLevel == "GlobalQueue":
        return getAgentInfoFromGlobalQueue(serviceURL)
    elif serviceLevel == "LocalQueue":
        return getAgentInfoFromLocalQueue(serviceURL)
    else:
        raise

def getAgentInfoFromReqMgr(serviceURL):
    """ get agent info from request mgr """

    reqMgr = RequestManager({'endpoint':serviceURL})
    #get information from global queue.
    try:
        gQueues = reqMgr.getWorkQueue()
    except Exception, ex:
        errorInfo = {}
        errorInfo['url'] = serviceURL
        errorInfo['status'] = "Request Manager down: %s" % serviceURL
        errorInfo['acdc'] = 'N/A'
        return [errorInfo]

    agents = []
    for queue in gQueues:
        agents.extend(getAgentInfoFromGlobalQueue(queue))
    return agents

def getAgentInfoFromGlobalQueue(serviceURL):

    globalQ = WorkQueue({'endpoint': serviceURL})
    try:
        childQueues = globalQ.getChildQueues()
    except Exception, ex:
        errorInfo = {}
        errorInfo['url'] = serviceURL
        errorInfo['status'] = "Global Queue down: %s" % serviceURL
        errorInfo['acdc'] = 'N/A'
        return [errorInfo]

    agents = []
    for childQueue in childQueues:
        agents.append(getAgentInfoFromLocalQueue(childQueue))
    return agents

def getAgentInfoFromLocalQueue(serviceURL):
    """ get agent status from local agent """

    agentInfo = {}
    agentURL = serviceURL.replace('/workqueue', '/wmagent')
    agentService = WMAgent({'endpoint': agentURL})
    try:
        agent = agentService.getAgentStatus(detail = False)
        status = agent['status']
        acdcURL = agentService.getACDCInfo()['url']
    except Exception, ex:
        status = "Agent Service down: %s" % agentURL
        acdcURL = 'N/A'

    agentInfo['url'] = agentURL
    agentInfo['status'] = status
    agentInfo['acdc'] = acdcURL
    return agentInfo
