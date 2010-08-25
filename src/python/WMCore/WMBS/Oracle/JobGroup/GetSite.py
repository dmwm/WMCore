#!/usr/bin/env python
"""
_GetSite_

Oracle implementation of JobGroup.GetSite
"""

__all__ = []
__revision__ = "$Id: GetSite.py,v 1.1 2009/07/01 19:27:52 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.GetSite import GetSite as MySQLGetSite

class GetSite(MySQLGetSite):
    """
    Right now, same as MySQL

    """
