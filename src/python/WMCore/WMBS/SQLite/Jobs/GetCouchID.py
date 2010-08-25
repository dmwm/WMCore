#!/usr/bin/env python
"""
_GetCouchID_

SQLite implementation of Jobs.GetCouchID
"""

__revision__ = "$Id: GetCouchID.py,v 1.1 2009/09/16 20:17:06 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetCouchID import GetCouchID as MySQLGetCouchID

class GetCouchID(MySQLGetCouchID):
    """
    Identical to MySQL version.
    """
    pass
