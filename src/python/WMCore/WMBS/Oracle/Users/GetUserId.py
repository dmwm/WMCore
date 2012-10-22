#!/usr/bin/env python
"""
_GetUserId_

Oracle implementation of Users.GetUserId
"""

from WMCore.WMBS.MySQL.Users.GetUserId import GetUserId as MySQLGetUserId

class GetUserId(MySQLGetUserId):
    """
    _GetUserId_

    Load user pars by ID
    """
