#!/usr/bin/env python
"""
_ProdMgrRetrieve_

API for the ProdMgr to pull in details about requests assigned to it


"""

import WMCore.RequestManager.RequestDB.Interface.Request.ListRequests as ListRequests
import WMCore.RequestManager.RequestDB.Interface.Request.ChangeState as ChangeStatus
import WMCore.RequestManager.RequestDB.Interface.Request.GetRequest as GetRequest


def findAssignedRequests(prodMgr):
    """
    _findAssignedRequests_

    find a list of assigned state requests associated to the prodMgr
    instance provided.

    returns a list of request IDs

    """
    pmReqs = ListRequests.listRequestsByProdMgr(prodMgr)

    result = [ x["RequestID"] for x in pmReqs
               if x['RequestStatus'] == "assigned" ]

    return result


def acceptRequest(requestId):
    """
    _acceptRequest_

    Method to tell ReqMgr that a request has been accepted by the PM
    it was assigned to

    """
    ChangeStatus.changeRequestIDStatus(requestId, "assigned-prodmgr")
    return



def getRequest(requestId):
    """
    _getRequest_

    Get the full details of the request to be passed to the PM.

    Includes
    - All basic request information from DB
    - Teams the request has been assigned to
    - Priority per assigned team
    - URL to download the workflow spec from

    TODO: Add download URL for the workflow spec.
          Follow up with FVL what needs to be added here

    """


    request = GetRequest.getRequest(requestId)
    assigned = GetRequest.getRequestAssignments(requestId)

    for team in assigned:
        team['TeamPriority'] += request['RequestPriority']
    request['Assignments'] = assigned


    return request


