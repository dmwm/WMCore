#!/usr/bin/env python
"""
_SetSite_

Oracle implementation of JobGroup.SetSite
"""

__all__ = []
__revision__ = "$Id: SetSite.py,v 1.1 2009/07/01 19:25:14 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.SetSite import SetSite as MySQLSetSite

class SetSite(MySQLSetSite):
    """
    Right now, same as MySQL

    """
