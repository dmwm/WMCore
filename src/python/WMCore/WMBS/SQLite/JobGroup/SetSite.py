#!/usr/bin/env python
"""
_GetSite_

SQLite implementation of JobGroup.GetSite
"""

__all__ = []
__revision__ = "$Id: SetSite.py,v 1.1 2009/07/01 19:25:15 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.SetSite import SetSite as MySQLSetSite

class GetSite(MySQLSetSite):
    """
    Right now, does nothing different.

    """
