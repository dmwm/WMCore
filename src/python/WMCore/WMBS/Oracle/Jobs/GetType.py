#!/usr/bin/env python
"""
_GetType_

Oracle implementation of Jobs.GetType
"""

__revision__ = "$Id: GetType.py,v 1.1 2009/10/13 20:52:41 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetType import GetType as MySQLGetType

class GetType(MySQLGetType):
    """
    Identical to MySQL version for now

    """
    pass
