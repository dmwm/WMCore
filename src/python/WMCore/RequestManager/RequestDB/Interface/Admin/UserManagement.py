#!/usr/bin/env python
"""
_UserManagement_

API for manipulating user information in the database

"""

import WMCore.RequestManager.RequestDB.Connection as DBConnect


def deleteUser(hnUsername):
    """
    _deleteUser_

    Delete the named user from the DB

    """

    factory = DBConnect.getConnection()
    userRemover = factory(classname = "Requestor.Delete")
    result = userRemover.execute(hnUsername)
    return result


def getPriority(hnUserName):
    """ Set the user priority to the amount given """
    factory = DBConnect.getConnection()
    userPriority = factory(classname = "Requestor.GetPriority")
    result = userPriority.execute(hnUserName)
    return result


def setPriority(hnUsername, priority):
    """
    _setPriority_

    Sets user priority to the integer amount specified

    """
    factory = DBConnect.getConnection()
    userPriority = factory(classname = "Requestor.SetPriority")
    result = userPriority.execute(hnUsername, priority)
    return result
