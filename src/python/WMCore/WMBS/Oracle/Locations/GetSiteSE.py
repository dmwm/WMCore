#!/usr/bin/env python
"""
_GetSiteSE_

Oracle implementation of Locations.GetSiteSE
"""

__all__ = []



from WMCore.WMBS.MySQL.Locations.GetSiteSE import GetSiteSE as MySQLGetSiteSE

class GetSiteSE(MySQLGetSiteSE):
    """
    Same as MySQL version

    """
