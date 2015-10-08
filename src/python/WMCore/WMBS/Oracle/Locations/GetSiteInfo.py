#!/usr/bin/env python
"""
_GetSiteInfo_

Oracle implementation of Locations.GetSiteInfo
"""

__all__ = []



from WMCore.WMBS.MySQL.Locations.GetSiteInfo import GetSiteInfo as MySQLGetSiteInfo

class GetSiteInfo(MySQLGetSiteInfo):
    """
    Same as MySQL version

    """
