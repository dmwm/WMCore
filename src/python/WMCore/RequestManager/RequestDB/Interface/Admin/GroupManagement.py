#!/usr/bin/env python
"""
_GroupManagement_

API for manipulating group information in the database

"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def addGroup(groupName, priority = 1):
    """
    _addGroup_

    Add a physics group with the name provided

    """
    factory = DBConnect.getConnection()
    groupAdd = factory(classname = "Group.New")
    result = groupAdd.execute(groupName, priority)
    return result

def addUserToGroup(userName, groupName):
    """
    _addUserToGroup_

    Add the named user to the named group
    """
    factory = DBConnect.getConnection()
    getUserId   = factory(classname = "Requestor.ID")
    getGroupId  = factory(classname = "Group.ID")
    userId = getUserId.execute(userName)
    groupId = getGroupId.execute(groupName)

    if groupId == None:
        msg = "Failed to add user %s to group %s\n" % (userName, groupName)
        msg += "Group: %s is not registered in Request Manager" % groupName
        raise RuntimeError, msg
    if userId == None:
        msg = "Failed to add user %s to group %s\n" % (userName, groupName)
        msg += "User: %s is not registered in Request Manager" % userName
        raise RuntimeError, msg

    newAssoc = factory(classname = "Requestor.NewAssociation")
    newAssoc.execute(userId, groupId)
    return

def removeUserFromGroup(userName, groupName):
    """
    _removeUserFromGroup_

    Remove the named user from the named group

    """
    factory = DBConnect.getConnection()
    getUserId   = factory(classname = "Requestor.ID")
    getGroupId  = factory(classname = "Group.ID")
    userId = getUserId.execute(userName)
    groupId = getGroupId.execute(groupName)

    if groupId == None:
        msg = "Failed to remove user %s from %s\n" % (userName, groupName)
        msg += "Group: %s is not registered in Request Manager" % groupName
        raise RuntimeError, msg
    if userId == None:
        msg = "Failed to remove user %s from %s\n" % (userName, groupName)
        msg += "User: %s is not registered in Request Manager" % userName
        raise RuntimeError, msg

    deleteAssoc = factory(classname = "Requestor.DeleteAssociation")
    deleteAssoc.execute(userId, groupId)
    return



def deleteGroup(groupName):
    """
    _deleteGroup_

    Delete the named group from the DB

    """
    factory = DBConnect.getConnection()
    groupRemover = factory(classname = "Group.Delete")
    result = groupRemover.execute(groupName)
    return result


def increasePriority(groupName, priorityIncrease):
    """
    _increasePriority_

    Increase groups priority by the integer amount specified

    """
    pass

def decreasePriority(groupName, priorityDecrease):
    """
    _decreasePriority_

    Decrease groups priority by the integer amount specified

    """
    pass




