#!/usr/bin/env python

"""
SQLite implementation of AddCKType
"""


__revision__ = "$Id: AddCKType.py,v 1.1 2009/12/02 20:04:23 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.AddCKType import AddCKType as MySQLAddCKType

class AddCKType(MySQLAddCKType):
    """
    Identical to MySQL version
    """
