#!/usr/bin/env python
"""
_ListRequests_

API Methods to list the requests in the database

"""




import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def listRequests():
    """
    _listRequests_

    Get a list of all request name/id pairs and status in the DB

    """
    factory = DBConnect.getConnection()
    reqFind = factory(classname = "Request.Find")
    result = reqFind.execute()
    return result




def listRequestsByStatus(statusName):
    """
    _listRequestsByStatus_

    Get a dict of request id/name pairs in the status provided

    """
    factory = DBConnect.getConnection()
    reqFind = factory(classname = "Request.FindByStatus")
    result = reqFind.execute(statusName)
    return result



def listRequestsForGroup(groupName, statusName = None):
    """
    _listRequestsForGroup_

    List all requests for a group, with optional status value

    """
    factory = DBConnect.getConnection()
    groupId = factory(classname = "Group.ID").execute(groupName)
    if groupId == None:
        msg = "Group %s not known to reqmgr database" % groupName
        raise RuntimeError, msg
    reqFind = factory(classname = "Request.FindByGroupStatus")
    result = reqFind.execute(groupId, statusName)
    return result


def listRequestsByTeam(teamName, statusName = None):
    """
    _listRequestsByTeam_

    Get a list of requests based on the production team they have been assigned to, with
    optional status selection


    """
    factory = DBConnect.getConnection()
    teamId = factory(classname = "Team.ID").execute(teamName)
    if teamId == None:
        msg = "Team %s not known to reqmgr database" % teamName
        raise RuntimeError, msg
    reqFind = factory(classname = "Request.FindByTeam")
    result = reqFind.execute(teamId, statusName)
    return result



def listRequestsByProdMgr(prodMgrName):
    """
    _listRequestsByProdMgr_

    List requests associated to a prodMgr

    """
    factory = DBConnect.getConnection()
    reqFind = factory(classname = "Request.FindByProdMgr")
    result = reqFind.execute(prodMgrName)
    return result





