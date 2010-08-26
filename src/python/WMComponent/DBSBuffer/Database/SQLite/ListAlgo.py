#!/usr/bin/env python
"""
_ListAlgo_

SQLite implementation of DBSBuffer.ListAlgo
"""

__revision__ = "$Id: ListAlgo.py,v 1.1 2009/07/13 19:44:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.ListAlgo import ListAlgo as MySQLListAlgo

class ListAlgo(MySQLListAlgo):
    """
    _ListAlgo_

    Retrieve information about an algorithm from the DBSBuffer.  This is mainly
    used by the unit tests to verify that the NewAlgo DAO is working correctly.
    """
    pass
