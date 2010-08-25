#!/usr/bin/env python
"""
_NewDataset_

SQLite implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.2 2009/07/13 19:37:59 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMComponent.DBSBuffer.Database.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):
    """
    _NewDataset_

    Add a new dataset to DBS Buffer
    """
    sql = """INSERT INTO dbsbuffer_dataset (path)
               SELECT :path WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_dataset WHERE path = :path)"""
