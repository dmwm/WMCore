#!/usr/bin/env python

"""
SQLite implementation of AddCKType
"""


__revision__ = "$Id: AddCKType.py,v 1.1 2009/12/02 19:35:36 mnorman Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.AddCKType import AddCKType as MySQLAddCKType

class AddCKType(MySQLAddCKType):
    """
    Identical to MySQL version
    """
                

