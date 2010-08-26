#!/usr/bin/env python
"""
_ListAlgo_

Oracle implementation of DBSBuffer.ListAlgo
"""

__revision__ = "$Id: ListAlgo.py,v 1.1 2009/07/14 19:20:42 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.ListAlgo import ListAlgo as MySQLListAlgo

class ListAlgo(MySQLListAlgo):
    """
    _ListAlgo_

    Retrieve information about an algorithm from the DBSBuffer.  This is mainly
    used by the unit tests to verify that the NewAlgo DAO is working correctly.
    """
    pass
