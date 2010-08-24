#!/usr/bin/env python
"""
_GetRequest_


API to get requests from the DB

"""



import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect
import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
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

def getRequestByName(requestName):
    factory = DBConnect.getConnection()
    f =  factory(classname = "Request.FindByName")
    id = f.execute(requestName)
    return getRequest(id)
     

def getRequestAssignments(requestId):
    """
    _getRequestAssignments_

    Get the assignments to production teams for a request

    """
    factory = DBConnect.getConnection()
    getAssign = factory(classname = "Assignment.GetByRequest")
    result = getAssign.execute(requestId)
    return result
