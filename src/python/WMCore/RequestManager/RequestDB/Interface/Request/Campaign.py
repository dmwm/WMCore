#!/usr/bin/env python

"""
Campaign-related methods for database access

"""

import WMCore.RequestManager.RequestDB.Connection as DBConnect
from cherrypy import HTTPError

def listRequestsByCampaign(campaignName):
    factory = DBConnect.getConnection()
    campaignFind = factory(classname = "Campaign.ID")
    campaignID = campaignFind.execute(campaignName)
    if campaignID == None or campaignID == []:
        raise HTTPError(404, "Cannot find campaign")
    reqFind = factory(classname = "Request.FindByCampaign")
    result = reqFind.execute(campaignID)
    return result

def listCampaigns():
    factory = DBConnect.getConnection()
    campaignFind = factory(classname = "Campaign.List")
    result = campaignFind.execute()
    return result

def addCampaign(campaignName):
    factory = DBConnect.getConnection()
    check = factory(classname = "Campaign.ID")
    checkResults = check.execute(campaignName)
    if checkResults == None or checkResults == []:
        add = factory(classname = "Campaign.New")
        add.execute(campaignName)
    return

def deleteCampaign(campaignName):
    factory = DBConnect.getConnection()
    deleter = factory(classname = "Campaign.Delete")
    result = deleter.execute(campaignName)
    return result

def associateCampaign(campaignName, requestID):
    factory = DBConnect.getConnection()
    campaignFind = factory(classname = "Campaign.ID")
    campaignID = campaignFind.execute(campaignName)
    if campaignID == None or campaignID == []:
        add = factory(classname = "Campaign.New")
        add.execute(campaignName)
        campaignID = campaignFind.execute(campaignName)
    add = factory(classname = "Campaign.NewAssoc")
    result = add.execute(requestID, campaignID)
    return result
