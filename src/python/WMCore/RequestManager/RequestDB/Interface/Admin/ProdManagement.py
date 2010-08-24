#!/usr/bin/env python
"""
_ProdManagement_

API for manipulating production system information in the database

Includes APIs to tweak
- Production Teams
- ProdMgr instances
- ProdAgent instances

"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect




def addTeam(teamName):
    """
    _addTeam_

    Add a new Operations team

    """
    factory = DBConnect.getConnection()
    check = factory(classname = "Team.List")
    checkResults = check.execute()
    if not teamName in checkResults.keys():
        addTeam = factory(classname = "Team.New")
        addTeam.execute(teamName)
    return


def removeTeam(teamName):
    """
    _removeTeam_

    Remove the named team from the DB

    TODO: Remove assignments to the team prior to removal

    """
    factory = DBConnect.getConnection()
    removeTeam = factory(classname = "Team.Delete").execute(teamName)
    return


def listTeams():
    """
    _listTeams_

    Show which teams exist
    """
    factory = DBConnect.getConnection()
    return factory(classname = "Team.List").execute()



def associateProdMgr(requestName, prodMgrUrl):
    """
    _associateProdMgr_

    Add an association between a prodmgr instance, given the contact url
    for the ProdMgr and the request name provided

    This is used to indicate to the ProdMgr which requests it needs to
    pick up


    """
    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID").execute(requestName)
    if requestId == None:
        msg = "Unknown Request Name: %s \n" % requestName
        msg += "Unable to associate ProdMgr for request"
        raise RuntimeError, msg


    addPM = factory(classname = "Progress.ProdMgr")
    addPM.execute(requestId, prodMgrUrl)
    return


def getProdMgr(requestName):
    """
    _associateProdMgr_

    Add an association between a prodmgr instance, given the contact url
    for the ProdMgr and the request name provided

    This is used to indicate to the ProdMgr which requests it needs to
    pick up


    """
    factory = DBConnect.getConnection()
    requestId = factory(classname = "Request.ID").execute(requestName)
    if requestId == None:
        msg = "Unknown Request Name: %s \n" % requestName
        msg += "Unable to associate ProdMgr for request"
        raise RuntimeError, msg

    getPM = factory(classname = "Progress.GetProdMgr")
    return getPM.execute(requestId)




def associateProdAgent(requestName, prodAgentUrl):
    """
    _associateProdAgent_

    Add an association between a PA instance and the request provided

    At present this is more for being able to go and get monitoring
    information and details from a PA.

    """
    pass



