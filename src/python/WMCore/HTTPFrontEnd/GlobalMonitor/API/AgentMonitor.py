import logging
### use request manager funtion directly
### TODO: remove this when GlobalMonitor spins out as a separate application
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest \
          import getGlobalQueues
###TODO: add back when GlobalMonitor spins out as a separate application
###from WMCore.Services.RequestManager.RequestManager import RequestManager
from WMCore.Services.WorkQueue.WorkQueue import WorkQueue
from WMCore.Services.WMAgent.WMAgent import WMAgent
from WMCore.HTTPFrontEnd.GlobalMonitor.API.DataFormatter import splitCouchServiceURL

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
    
    ###TODO: add back when GlobalMonitor spins out as a separate application
    ###reqMgr = RequestManager({'endpoint':serviceURL})
    #get information from global queue.
    try:
        ###TODO: add back when GlobalMonitor spins out as a separate application
        gQueues = getGlobalQueues()
        ###TODO: add back when GlobalMonitor spins out as a separate application
        ###gQueues = reqMgr.getWorkQueue()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
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

    url, dbName = splitCouchServiceURL(serviceURL)
    globalQ = WorkQueue(url, dbName)
    try:
        childQueues = globalQ.getChildQueues()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        errorInfo = {}
        errorInfo['url'] = serviceURL
        errorInfo['status'] = "Global Queue down: %s" % serviceURL
        errorInfo['acdc'] = 'N/A'
        return [errorInfo]

    agents = []
    for childQueue in childQueues:
        agents.extend(getAgentInfoFromLocalQueue(childQueue))
    return agents

def getAgentInfoFromLocalQueue(serviceURL):
    """ get agent status from local agent """
    url, dbName = splitCouchServiceURL(serviceURL)
    localQ = WorkQueue(url, dbName)

    try:
        wmbsUrl = localQ.getWMBSUrl()
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        errorInfo = {}
        errorInfo['url'] = serviceURL
        errorInfo['status'] = "Local Queue down: %s" % serviceURL
        errorInfo['acdc'] = 'N/A'
        return errorInfo

    agents = []
    for url in wmbsUrl:
        agents.append(getAgentInfoFromWMBS(url))
    return agents

def getAgentInfoFromWMBS(serviceURL):
    agentInfo = {}
    agentURL = serviceURL.replace('wmbsservice/wmbs', 'wmbsservice/wmagent')
    agentService = WMAgent({'endpoint': agentURL})
    try:
        agent = agentService.getAgentStatus(detail = False)
        status = agent['status']
        acdcURL = agentService.getACDCInfo()['url']
    except Exception, ex:
        logging.warning("Error: %s" % str(ex))
        status = "Agent Service down: %s" % agentURL
        acdcURL = 'N/A'

    agentInfo['url'] = agentURL
    agentInfo['status'] = status
    agentInfo['acdc'] = acdcURL
    return agentInfo
