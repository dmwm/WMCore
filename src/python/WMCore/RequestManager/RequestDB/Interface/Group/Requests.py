#!/usr/bin/env python
"""
_Requests_

Methods to check on the requests belonging to a group

"""




import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def listRequests(groupName):
    """
    _listRequests_

    return a map of request name/ids for the group provided

    """
    factory = DBConnect.getConnection()
    getGroupId = factory(classname = "Group.ID")


    groupId = getGroupId.execute(groupName)
    if groupId == None:
        msg = "Group: %s not registered with Request Manager" % groupName
        raise RuntimeError, msg

    listReqs = factory(classname = "Group.ListRequests")

    result = listReqs.execute(groupId)
    return result




