#!/usr/bin/env python
"""
_ListDataset_

SQLite implementation of DBSBuffer.ListDataset
"""

__revision__ = "$Id: ListDataset.py,v 1.1 2009/07/13 19:44:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.ListDataset import ListDataset as MySQLListDataset

class ListDataset(MySQLListDataset):
    """
    _ListDataset_

    Retrieve information about a dataset in the DBSBuffer.  This is mostly used
    by the unit tests.
    """
    pass
