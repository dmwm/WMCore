#!/usr/bin/env python
"""
_SoftwareManagement_

API for manipulating software information in the database

"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect



def addSoftware(softwareName):
    """
    _addSoftware_

    Add a software version to the database

    """
    factory = DBConnect.getConnection()
    checkSw = factory(classname = "Software.ID")
    checkResults = checkSw.execute(softwareName)
    if checkResults == None or checkResults == []:
        addSw = factory(classname = "Software.New")
        addSw.execute(softwareName)
    return

def listSoftware():
    """
    _listSoftware_
    
    lists all software versions in the database
    """
    factory = DBConnect.getConnection()
    listSw = factory(classname = "Software.List")
    return listSw.execute()


def removeSoftware(softwareName):
    """
    _removeSoftware_

    remove a software version from the database

    """
    factory = DBConnect.getConnection()
    removeSw = factory(classname = "Software.Delete")
    removeSw.execute(softwareName)
    return

