#!/usr/bin/env python
"""
_ChangeState_

State Change API methods


"""
import logging
from cherrypy import HTTPError

from WMCore.Wrappers.JsonWrapper import JSONEncoder
import WMCore.RequestManager.RequestDB.Connection as DBConnect
from WMCore.Services.WMStats.WMStatsWriter import WMStatsWriter
from WMCore.Database.CMSCouch import Database

# TODO: Merge with getRequest.requestID
def getRequestID(factory, requestName):
    reqId = factory(classname = "Request.ID").execute(requestName)
    if reqId == None:
        raise HTTPError(404, 'Given requestName not found')
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
        raise RuntimeError(msg)

    stateChanger = factory(classname = "Request.SetStatus")
    stateChanger.execute(requestId, statusId)

    if priority != None:
        priorityChange = factory(classname = "Request.Priority")
        priorityChange.execute(requestId, priority)


def changeRequestPriority(requestName, priority):
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    priorityChange = factory(classname = "Request.Priority")
    priorityChange.execute(reqId, priority)


def changeRequestStatus(requestName, newState, priority=None, wmstatUrl=None):
    """
    _changeRequestStatus_

    Basic API to change a request to a new state, also
    includes optional priority change for the request

    - *requestName* : name of the request to be modified
    - *newState*    : name of the new status for the request
    - *priority* : optional integer priority
    
    Apparently when changing request state (on assignment page),
    it's possible to change priority at one go. Hence the argument is here.

    """
    # MySQL/Oracle
    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)
    changeRequestIDStatus(reqId, newState, priority)
    
    # CouchDB
    # have to first get information where the request Couch document is,
    # extracting the information from reqmgr_request.workflow table field
    reqData = factory(classname = "Request.Get").execute(reqId)
    # this would be something like this:
    # http://localhost:5984/reqmgr_workload_cache/maxa_RequestString-OVERRIDE-ME_130306_205649_8066/spec
    wfUrl = reqData['workflow']
    # cut off /maxa_RequestString-OVERRIDE-ME_130306_205649_8066/spec
    couchUrl = wfUrl.replace('/' + requestName + "/spec", '')
    couchDbName = couchUrl[couchUrl.rfind('/') + 1:]
    # cut off database name from the URL 
    url = couchUrl.replace('/' + couchDbName, '')
    couchDb = Database(couchDbName, url)
    fields = {"RequestStatus": newState}
    couchDb.updateDocument(requestName, "ReqMgr", "updaterequest", fields=fields, useBody=True) 

    #TODO: should we make this mendatory?
    if wmstatUrl:
        wmstatSvc = WMStatsWriter(wmstatUrl)
        wmstatSvc.updateRequestStatus(requestName, newState)
    

def assignRequest(requestName, teamName, prodMgr = None, wmstatUrl = None):
    """
    _assignRequest_

    Assign a request to a team.

    This does the following:

    - Changes the status to assigned
    - Creates an association to the team provided
    - Optionally associates the request to a prod mgr instance

    """

    factory = DBConnect.getConnection()
    reqId = getRequestID(factory, requestName)

    teamId = factory(classname = "Team.ID").execute(teamName)
    if teamId == None:
        msg = "Team named %s not known in database" % teamName
        msg += "Failed to assign request %s to team %s" % (requestName, teamName)
        raise RuntimeError(msg)

    if wmstatUrl:
        wmstatSvc = WMStatsWriter(wmstatUrl)
        wmstatSvc.updateTeam(requestName, teamName)

    assigner = factory(classname = "Assignment.New")
    assigner.execute(reqId, teamId)

    changeRequestStatus(requestName, 'assigned', priority = None, wmstatUrl = wmstatUrl)

    if prodMgr != None:
        addPM = factory(classname = "Progress.ProdMgr")
        addPM.execute(reqId, prodMgr)


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
    #return factory(classname = "Progress.Message").execute(reqId, message)
    message = message[:999]
    factory(classname = "Progress.Message").execute(reqId, message)
