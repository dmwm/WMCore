#!/usr/bin/env python
"""
_SetSite_

Oracle implementation of JobGroup.SetSite
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.SetSite import SetSite as MySQLSetSite

class SetSite(MySQLSetSite):
    """
    Right now, same as MySQL

    """
