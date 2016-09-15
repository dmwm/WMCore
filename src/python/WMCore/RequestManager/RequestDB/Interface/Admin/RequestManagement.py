"""
_RequestManagement_

API for administering requests in the database

"""

import WMCore.RequestManager.RequestDB.Connection as DBConnect
from WMCore.RequestManager.RequestDB.Interface.Request.GetRequest import requestID


def deleteRequest(requestName):
    """
    _deleteRequest_

    delete the request from the database by its RequestName
    """
    reqId = requestID(requestName)

    factory = DBConnect.getConnection()
    deleteReq = factory(classname="Request.Delete")
    deleteReq.execute(reqId)
