#!/usr/bin/env python
"""
_Requests_

Methods to check on the requests belonging to a user

"""
__revision__ = "$Id: Requests.py,v 1.1 2010/07/01 18:36:02 rpw Exp $"
__version__ = "$Revision: 1.1 $"


import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def listRequests(userName):
    """
    _listRequests_

    return a map of request name/ids for the user provided

    """
    factory = DBConnect.getConnection()
    getUserId = factory(classname = "Requestor.ID")


    userId = getUserId.execute(userName)
    if userId == None:
        msg = "User: %s not registered with Request Manager" % userName
        raise RuntimeError, msg

    listReqs = factory(classname = "Requestor.ListRequests")

    result = listReqs.execute(userId)
    return result




