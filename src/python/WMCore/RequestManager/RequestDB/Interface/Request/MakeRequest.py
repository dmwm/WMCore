#!/usr/bin/env python
"""
_MakeRequest_

API for creating a new request in the database

"""





import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect



def createRequest(hnUser, groupName, requestName, requestType, workflowName):
    """
    _createRequest_

    Create a new request entry, return the request id

    This creates a basic request entity, details of size of request, datasets
    etc need to be added with other API calls

    """
    factory = DBConnect.getConnection()
    getUserId = factory(classname = "Requestor.ID")

    #  //
    # // validate:
    #//  1. user is registered
    #  //2. user is in requested group
    # // 3. requested group exists
    #//
    userId = getUserId.execute(hnUser)
    if userId == None:
        msg = "User: %s not registered with Request Manager" % hnUser
        raise RuntimeError, msg

    getGroups = factory(classname = "Requestor.GetAssociationNames")
    groups = getGroups.execute(userId)
    if groupName not in groups.keys():
        allGroups  = factory(classname = "Group.List").execute()
        if groupName not in allGroups:
            msg = "No group named %s exists in Request Manager\n" % groupName
            msg += "Known Groups: %s" % allGroups
        else:
            msg = "User %s is not a member of group %s\n" % (hnUser, groupName)
            msg += "User is associated to groups: %s" % groups.keys()
        raise RuntimeError, msg
    associationId = groups[groupName]

    #  //
    # // get status id and type ids
    #//
    statusMap = factory(classname = "ReqStatus.Map").execute()
    typeMap = factory(classname = "ReqTypes.Map").execute()

    if requestType not in typeMap.keys():
        msg = "Unknown Request Type: %s\n" % requestType
        msg += "Known Types are %s" % typeMap.keys()
        raise RuntimeError, msg

    #  //
    # // does the request name already exist?
    #//
    requestId = factory(classname = "Request.ID").execute(requestName)
    if requestId != None:
        msg = "Request name already exists: %s\n" % requestName
        msg += "Cannot create new request with same name"
        raise RuntimeError, msg

    newRequest = factory(classname = "Request.New")
    try:
        reqId = newRequest.execute(
            request_name = requestName,
            request_type = typeMap[requestType],
            request_status = statusMap['new'],
            association_id = groups[groupName],
            workflow = workflowName
            )
    except Exception, ex:
        msg = "Unable to create request named %s\n" % requestName
        msg += str(ex)
        raise RuntimeError, msg
    return reqId



def associateInputDataset(requestName, datasetName, datasetType = "source"):
    """
    _associateInputDataset_

    Attach an input dataset to the named request

    """
    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID")

    reqId = requestId.execute(requestName)
    if reqId == None:
        msg = "Unknown Request: %s\n" % requestName
        msg += "Cannot associate dataset to request"
        raise RuntimeError, msg

    addDataset = factory(classname = "Datasets.NewInput")
    addDataset.execute(reqId, datasetName, datasetType)
    return


def associateOutputDataset(requestName, datasetName):
    """
    _associateOutputDataset_

    Attach an output dataset to the named request

    """
    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID")

    reqId = requestId.execute(requestName)
    if reqId == None:
        msg = "Unknown Request: %s\n" % requestName
        msg += "Cannot associate dataset to request"
        raise RuntimeError, msg

    addDataset = factory(classname = "Datasets.NewOutput")
    addDataset.execute(reqId, datasetName)
    return


def associateSoftware(requestName, softwareName):
    """
    _associateSoftware_

    associate software name to named request
    Software must be registered in the DB

    """

    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID")

    reqId = requestId.execute(requestName)
    if reqId == None:
        msg = "Unknown Request: %s\n" % requestName
        msg += "Cannot associate software to request"
        raise RuntimeError, msg

    softwareId = factory(classname = "Software.ID")
    swId = softwareId.execute(softwareName)
    if swId == None or swId == []:
        msg = "Unknown Software name: %s\n" % softwareName
        msg += "Cannot associate software to request"
        raise RuntimeError, msg


    softwareAssoc = factory(classname = "Software.Association")

    try:
        softwareAssoc.execute(reqId, swId[0])
    except Exception, ex:
        msg = "Unable to associate software to request\n"
        msg += "request: %s software: %s " % (requestName, softwareName)
        raise RuntimeError, msg

    return



def updateRequestSize(requestName, reqEventsSize, reqFilesSize = None):
    """
    _updateRequestSize_

    Update the size of the request in events to be generated/read
    and optionally  the number of files to be read for processing
    requests

    """
    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID")

    reqId = requestId.execute(requestName)
    if reqId == None:
        msg = "Unknown Request: %s\n" % requestName
        msg += "Cannot update size of request"
        raise RuntimeError, msg


    updateSize = factory(classname = "Request.Size")
    updateSize.execute(reqId, reqEventsSize, reqFilesSize)

    return


