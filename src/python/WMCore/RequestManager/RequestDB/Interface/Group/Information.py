#!/usr/bin/env python
"""
_Group.Information_

Get information about a Group

"""


import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def groupExists(groupName):
    """
    _groupExists_

    Return the ID of the group if it exists, otherwise return None

    """
    factory = DBConnect.getConnection()
    groupId = factory(classname = "Group.ID")
    result = groupId.execute(groupName)
    return result

def listGroups():
    """
    _listGroups_

    return a list of known group names

    """
    factory = DBConnect.getConnection()
    groupId = factory(classname = "Group.List")
    result = groupId.execute()
    return result

def usersInGroup(groupName):
    """
    _usersGroups_

    return a list of users registered to this group

    """
    result = []
    factory = DBConnect.getConnection()
    groupAssocs = factory(classname = "Group.GetAssociation")
    groupId = groupExists(groupName)
    userIds = groupAssocs.execute(groupId)
    userNames =  factory(classname = "Requestor.NameFromID")
    for userId in userIds:
       result.append(userNames.execute(userId))
    return result


def groupsForUser(userName):
    """ Return a list of all the groups this user belongs to """
    result = []
    factory = DBConnect.getConnection()
    getUserId = factory(classname = "Requestor.ID")
    userId = getUserId.execute(userName)
    getGroups = factory(classname = "Requestor.GetAssociationNames")
    groups = getGroups.execute(userId)
    return groups;

