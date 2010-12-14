#!/usr/bin/env python
"""
_ListAlgo_

SQLite implementation of DBSBuffer.ListAlgo
"""




from WMComponent.DBSBuffer.Database.MySQL.ListAlgo import ListAlgo as MySQLListAlgo

class ListAlgo(MySQLListAlgo):
    """
    _ListAlgo_

    Retrieve information about an algorithm from the DBSBuffer.  This is mainly
    used by the unit tests to verify that the NewAlgo DAO is working correctly.
    """
    pass
