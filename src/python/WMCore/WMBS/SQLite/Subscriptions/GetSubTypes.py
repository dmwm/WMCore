#!/usr/bin/env python
"""
_GetSubTypes_

SQLite implementation of Jobs.GetSubTypes
"""

__all__ = []



import logging

from WMCore.WMBS.MySQL.Subscriptions.GetSubTypes import GetSubTypes as MySQLGetSubTypes



class GetSubTypes(MySQLGetSubTypes):
    """
    Identical to MySQL version

    """
