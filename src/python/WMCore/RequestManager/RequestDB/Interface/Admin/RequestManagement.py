"""
_RequestManagement_

API for administering requests in the database

"""

import WMCore.RequestManager.RequestDB.Connection as DBConnect
from cherrypy import HTTPError



def deleteRequest(requestName):
    """
    _deleteRequest_

    delete the request from the database by its RequestName

    """
    factory = DBConnect.getConnection()
    finder =  factory(classname="Request.FindByName")
    reqId = finder.execute(requestName)
    if reqId == None:
        raise HTTPError(404, 'Given requestName not found: %s' % requestName)

    factory = DBConnect.getConnection()
    deleteReq = factory(classname = "Request.Delete")
    deleteReq.execute(reqId)