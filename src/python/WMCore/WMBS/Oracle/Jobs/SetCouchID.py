#!/usr/bin/env python
"""
_SetCouchID_

Oracle implementation of Jobs.SetCouchID
"""

__revision__ = "$Id: SetCouchID.py,v 1.1 2009/09/16 20:17:06 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.SetCouchID import SetCouchID as MySQLSetCouchID

class SetCouchID(MySQLSetCouchID):
    """
    Identical to MySQL version.
    """
    pass
