#!/usr/bin/env python
"""
_UserManagement_

API for manipulating user information in the database

"""

import logging
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


def increasePriority(hnUsername, priorityIncrease):
    """
    _increasePriority_

    Increase users priority by the integer amount specified

    """
    pass

def decreasePriority(hnUsername, priorityDecrease):
    """
    _decreasePriority_

    Decrease users priority by the integer amount specified

    """
    pass




