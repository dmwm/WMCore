#!/usr/bin/env python
"""
_GetRequest_


API to get requests from the DB

"""



import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeState
from WMCore.RequestManager.DataStructs.Request import Request


def getRequest(requestId):
    """
    _getRequest_


    retrieve a request based on the request id,
    return a ReqMgr.DataStructs.Request instance containing
    the information

    """
    factory = DBConnect.getConnection()
    reqGet = factory(classname = "Request.Get")
    reqTypes = factory(classname = 'ReqTypes.Map').execute()
    reqStatus = factory(classname = 'ReqStatus.Map').execute()

    reqData = reqGet.execute(requestId)

    reverseTypes = {}
    [ reverseTypes.__setitem__(v, k) for k, v in reqTypes.iteritems() ]
    reverseStatus = {}
    [ reverseStatus.__setitem__(v, k) for k, v in reqStatus.iteritems() ]

    getGroup = factory(classname = "Group.GetGroupFromAssoc")
    groupData = getGroup.execute(reqData['requestor_group_id'])

    getUser = factory(classname = "Requestor.GetUserFromAssoc")
    userData = getUser.execute(reqData['requestor_group_id'])
    requestInstance = Request()
    requestInstance["ReqMgrRequestID"] = reqData['request_id']
    requestInstance["RequestName"] = reqData['request_name']
    requestInstance["RequestType"] = reverseTypes[reqData['request_type']]
    requestInstance["RequestStatus"] = reverseStatus[reqData['request_status']]
    requestInstance["RequestPriority"] = reqData['request_priority']
    requestInstance["ReqMgrRequestBasePriority"] = reqData['request_priority']
    requestInstance["RequestWorkflow"] = reqData['workflow']
    requestInstance["RequestSizeEvents"] = reqData['request_size_events']
    requestInstance["RequestSizeFiles"] = reqData['request_size_files']

    requestInstance["Group"] = groupData['group_name']
    requestInstance["ReqMgrGroupID"] = groupData['group_id']
    requestInstance["ReqMgrGroupBasePriority"] = \
                        groupData['group_base_priority']
    requestInstance["Requestor"] = userData['requestor_hn_name']
    requestInstance["ReqMgrRequestorID"] = userData['requestor_id']
    requestInstance["ReqMgrRequestorBasePriority"] = \
                                userData['requestor_base_priority']
    requestInstance["RequestPriority"] = \
      requestInstance['RequestPriority'] + groupData['group_base_priority']
    requestInstance["RequestPriority"] = \
      requestInstance['RequestPriority'] + userData['requestor_base_priority']


    # TODO: Update priority for group and user values


    # get datasets and sw
    sqDeps = factory(classname = "Software.GetByAssoc")
    swVers = sqDeps.execute(requestId)
    requestInstance['SoftwareVersions'] = swVers.values()

    getDatasetsIn = factory(classname = "Datasets.GetInput")
    getDatasetsOut = factory(classname = "Datasets.GetOutput")

    datasetsIn = getDatasetsIn.execute(requestId)
    datasetsOut = getDatasetsOut.execute(requestId)

    requestInstance['InputDatasetTypes'] = datasetsIn
    requestInstance['InputDatasets'] = datasetsIn.keys()
    requestInstance['OutputDatasets'] = datasetsOut

    return requestInstance

def requestID(requestName):
    """ Finds the ReqMgr database ID for a request """
    factory = DBConnect.getConnection()
    f =  factory(classname = "Request.FindByName")
    id = f.execute(requestName)
    if id == None:
        raise RuntimeError, "Cannot find request %s" % requestName
    return id

def getRequestByName(requestName):
    return getRequest(requestID(requestName))

def getRequestByPrepID(prepID):
    factory = DBConnect.getConnection()
    getID = factory(classname = "Request.FindByPrepID")
    requestID = getID.execute(prepID)
    if requestID == None:
        return None
    return getRequest(requestID)
    
def getRequestDetails(requestName):
    """ Return a dict with the intimate details of the request """
    request = getRequestByName(requestName)
    request['Assignments'] = getAssignmentsByName(requestName)
    # show the status and messages
    request['RequestMessages'] = ChangeState.getMessages(requestName)
    # updates
    request['RequestUpdates'] = ChangeState.getProgress(requestName)
    # it returns a datetime object, which I can't pass through
    request['percent_complete'] = 0
    request['percent_success'] = 0
    for update in request['RequestUpdates']:
        update['update_time'] = str(update['update_time'])
        if update.has_key('percent_complete'):
            request['percent_complete'] = update['percent_complete']
        if update.has_key('percent_success'):
            request['percent_success'] = update['percent_success']
    return request

def getAllRequestDetails():
    requests = ListRequests.listRequests()
    result = []
    for request in requests:
        requestName = request['RequestName']
        details = getRequestDetails(requestName)
        # take out excessive information
        del details['RequestUpdates']
        del details['RequestMessages']
        result.append(details)
    return result 


def getRequestAssignments(requestId):
    """
    _getRequestAssignments_

    Get the assignments to production teams for a request

    """
    factory = DBConnect.getConnection()
    getAssign = factory(classname = "Assignment.GetByRequest")
    result = getAssign.execute(requestId)
    return result

def getAssignmentsByName(requestName):
    request = getRequestByName(requestName)
    reqID = request['ReqMgrRequestID']
    assignments = getRequestAssignments(reqID)
    return [assignment['TeamName'] for assignment in assignments]


def getOverview():
    """
    _getOverview_

    Get the status, type and global queue info for all the request

    """
    factory = DBConnect.getConnection()
    getSummary = factory(classname = "Request.GetOverview")
    result = getSummary.execute()
    return result

def getGlobalQueues():
    """
    _getGlobaQueues_

    Get list of Global Queues from request mgr db
    Convert Global Queue monitoring address to GlobalQueue
    Service address
    """
    factory = DBConnect.getConnection()
    getQueues = factory(classname = "Request.GetGlobalQueues")
    results = getQueues.execute()
    queues = []
    for url in results:
        queues.append(url.replace('workqueuemonitor', 'workqueue'))
    return queues

