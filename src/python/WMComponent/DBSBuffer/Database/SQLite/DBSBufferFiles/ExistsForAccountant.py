#!/usr/bin/env python
"""
_ExistsForAccountant_

SQLite implementation of Files.ExistsForAccountant
"""

__all__ = []
__revision__ = "$Id: ExistsForAccountant.py,v 1.1 2010/05/24 20:35:27 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.ExistsForAccountant import ExistsForAccountant as MySQLExistsForAccountant

class ExistsForAccountant(MySQLExistsForAccountant):
    """
    This is highly specialized.  You shouldn't confuse it with
    a normal Exists DAO
    """

