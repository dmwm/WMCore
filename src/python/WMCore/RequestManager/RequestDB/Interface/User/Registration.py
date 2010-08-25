#!/usr/bin/env python
"""
_Registration_

API calls for interacting with Request DB to:
- register as a user
- check details of a user
- check that a user is registered


"""

import logging
import WMCore.RequestManager.RequestDB.Connection as DBConnect


def isRegistered(hnUsername):
    """
    _isRegistered_

    return user ID is the HN username provided is registered as a
    reqmgr requestor, or None if not

    """
    factory = DBConnect.getConnection()
    userId = factory(classname = "Requestor.ID")
    result = userId.execute(hnUsername)
    return result


def registerUser(hnUsername, emailAddress, dnName = None):
    """
    _registerUser_

    Register a new user given:

    - CMS Hypernews username
    - contact email address (request related messages will be sent to this)
    - physics group name
    - (optional) certificate distingushing name

    The ID of the user will be returned, or an exception will be thrown
    if the user cannot be registered

    """
    logging.info("Adding User: %s" % hnUsername)
    factory = DBConnect.getConnection()


    newUser = factory(classname = "Requestor.New")
    try:
        newUser.execute(hnUsername, emailAddress,dnName , 1)
    except Exception, ex:
        msg = "Unable to register user:\n%s" % str(ex)
        raise RuntimeError, msg


def listUsers():
    """
    _listUsers_

    return a list of known user names

    """
    factory = DBConnect.getConnection()
    users = factory(classname = "Requestor.List")
    result = users.execute()
    return result

