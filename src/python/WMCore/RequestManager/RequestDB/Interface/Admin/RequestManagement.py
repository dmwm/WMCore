#!/usr/bin/env python
"""
_RequestManagement_

API for administering requests in the database

"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def deleteRequest(requestId):
    """
    _deleteRequest_

    delete the request from the database by its database ID

    """

    factory = DBConnect.getConnection()
    deleteReq = factory(classname = "Request.Delete")
    deleteReq.execute(requestId)
    return

