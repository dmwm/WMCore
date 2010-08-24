#!/usr/bin/env python
"""
_ChangeState_

State Change API methods


"""



import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def getRequestID(factory, requestName):
    reqId = factory(classname = "Request.ID").execute(requestName)
    if reqId == None:
        msg = "Request Named %s not known in database" % requestName
        raise RuntimeError, msg
    return reqId


def changeRequestIDStatus(requestId, newState, priority = None):
    """
    _changeRequestIDStatus_

    Basic API to change a request to a new state, also
    includes optional priority change for the request

    - *requestId* : id of the request to be modified
    - *newState*    : name of the new status for the request
    - *priority* : optional integer priority 

    """
    factory = DBConnect.getConnection()
    statusMap = factory(classname = "ReqStatus.Map").execute()
    statusId = statusMap.get(newState, None)
    if statusId == None:
        msg = "Attempted to change request %s to unknown status value %s" % (
            requestId, newState)
        raise RuntimeError, msg

    stateChanger = factory(classname = "Request.SetStatus")
    stateChanger.execute(requestId, statusId)

    if priority != None:
        priorityChange = factory(classname = "Request.Priority")
        priorityChange.execute(requestId, priority)

    return


def changeRequestStatus(requestName, newState, priority = None):
    """
    _changeRequestStatus_

    Basic API to change a request to a new state, also
    includes optional priority change for the request

    - *requestName* : name of the request to be modified
    - *newState*    : name of the new status for the request
    - *priority* : optional integer priority

    """
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    changeRequestIDStatus(reqId, newState, priority)
    return


def assignRequest(requestName, teamName, priorityModifier = 0, prodMgr = None):
    """
    _assignRequest_

    Assign a request to a team.

    This does the following:

    - Changes the status to assigned
    - Creates an association to the team provided
    - Optionally associates the request to a prod mgr instance
    - Optionally sets the priority modifier for the team (allows same request to be
      shared between two teams with different priorities


    """
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    statusMap = factory(classname = "ReqStatus.Map").execute()
    statusId = statusMap['assigned']

    teamId = factory(classname = "Team.ID").execute(teamName)
    if teamId == None:
        msg = "Team named %s not known in database" % teamName
        msg += "Failed to assign request %s to team %s" % (requestName, teamName)
        raise RuntimeError, msg



    assigner = factory(classname = "Assignment.New")
    assigner.execute(reqId, teamId, priorityModifier)

    stateChanger = factory(classname = "Request.SetStatus")
    stateChanger.execute(reqId, statusId)



    if prodMgr != None:
        addPM = factory(classname = "Progress.ProdMgr")
        addPM.execute(reqId, prodMgr)

    return

def deleteAssignment(requestName):
    """
    """
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    delete = factory(classname = "Assignment.Delete").execute(reqId)

def updateRequest(requestName, paramDict):
    """
    _updateRequest_
    Add a progress update to teh request Id provided, params can
    optionally contain:

    - *events_written* Int
    - *events_merged*  Int
    - *files_written*  Int
    - *files_merged*   int
    - *percent_success* float
    - *percent_completed* float
    - *dataset*        string (dataset name)
    """

    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    factory(classname = "Progress.Update").execute(reqId, **paramDict)
    return


def getProgress(requestName):
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    return factory(classname = "Progress.GetProgress").execute(reqId)


def getMessages(requestName):
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    return factory(classname = "Progress.GetMessages").execute(reqId)


def putMessage(requestName, message):
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    factory(classname = "Progress.Message").execute(reqId, message)
    return

