#!/usr/bin/env python
"""
_ListDataset_

Oracle implementation of DBSBuffer.ListDataset
"""




from WMComponent.DBS3Buffer.MySQL.ListDataset import ListDataset as MySQLListDataset

class ListDataset(MySQLListDataset):
    """
    _ListDataset_

    Retrieve information about a dataset in the DBSBuffer.  This is mostly used
    by the unit tests.
    """
    pass
