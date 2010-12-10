"""
_GetUserId_

Oracle implementation of Users.GetUserId
"""

__all__ = []



import time
from WMCore.WMBS.Workflow.MySQL.Users.GetUserId import GetUserId as MySQLGetUserId

class GetUserId(MySQLGetUserId):
    """
    _GetUserId_

    Load user pars by ID
    """

