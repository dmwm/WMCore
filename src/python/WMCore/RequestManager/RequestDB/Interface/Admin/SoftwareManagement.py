#!/usr/bin/env python
"""
_SoftwareManagement_

API for manipulating software information in the database

"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect

def addSoftware(softwareName, scramArch = None):
    """
    _addSoftware_

    Add a software version to the database

    """
    factory = DBConnect.getConnection()
    checkDict = listSoftware()
    if scramArch in checkDict and softwareName in checkDict[scramArch]:
        return
    else:
        addSw = factory(classname = "Software.New")
        addSw.execute(softwareNames = [softwareName], scramArch = scramArch)
    return

def updateSoftware(softwareNames, scramArch = None):
    """
    _updateSoftware_

    Add a software version to the database if it does not already exist.
    If a version exists that is not added, delete it.
    """
    versionsToAdd = []
    versionsToDel = []
    factory = DBConnect.getConnection()
    currentVersions = listSoftware()

    if not scramArch in currentVersions.keys():
        # Then the whole architecture is new.  Add it.
        versionsToAdd = softwareNames
    else:
        scramVersions = currentVersions[scramArch]
        versionsToAdd = list(set(softwareNames) - set(scramVersions))
        versionsToDel = list(set(scramVersions) - set(softwareNames))

    if len(versionsToAdd) > 0:
        addSw = factory(classname = "Software.New")
        addSw.execute(softwareNames = versionsToAdd, scramArch = scramArch)
    if len(versionsToDel) > 0:
        for version in versionsToDel:
            removeSoftware(softwareName = version, scramArch = scramArch)
    return

def listSoftware():
    """
    _listSoftware_

    lists all software versions in the database
    """
    factory = DBConnect.getConnection()
    listSw = factory(classname = "Software.List")
    return listSw.execute()


def removeSoftware(softwareName, scramArch = None):
    """
    _removeSoftware_

    remove a software version from the database

    """
    factory = DBConnect.getConnection()
    removeSw = factory(classname = "Software.Delete")
    removeSw.execute(softwareName, scramArch = scramArch)
    return
