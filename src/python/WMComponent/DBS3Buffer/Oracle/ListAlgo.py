#!/usr/bin/env python
"""
_ListAlgo_

Oracle implementation of DBSBuffer.ListAlgo
"""




from WMComponent.DBS3Buffer.MySQL.ListAlgo import ListAlgo as MySQLListAlgo

class ListAlgo(MySQLListAlgo):
    """
    _ListAlgo_

    Retrieve information about an algorithm from the DBSBuffer.  This is mainly
    used by the unit tests to verify that the NewAlgo DAO is working correctly.
    """
    pass
