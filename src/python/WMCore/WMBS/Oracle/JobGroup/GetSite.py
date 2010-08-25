#!/usr/bin/env python
"""
_GetSite_

Oracle implementation of JobGroup.GetSite
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.GetSite import GetSite as MySQLGetSite

class GetSite(MySQLGetSite):
    """
    Right now, same as MySQL

    """
