#!/usr/bin/env python
"""
_GetSiteInfo_

SQLite implementation of Locations.GetSiteInfo
"""

__all__ = []



from WMCore.WMBS.MySQL.Locations.GetSiteInfo import GetSiteInfo as MySQLGetSiteInfo

import logging

class GetSiteInfo(MySQLGetSiteInfo):
    """
    Same as MySQL version

    """
