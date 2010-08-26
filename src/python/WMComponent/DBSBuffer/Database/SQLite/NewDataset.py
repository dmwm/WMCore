#!/usr/bin/env python
"""
_NewDataset_

SQLite implementation of DBSBuffer.NewDataset
"""

__revision__ = "$Id: NewDataset.py,v 1.3 2010/02/09 17:28:56 meloam Exp $"
__version__ = "$Revision: 1.3 $"

from WMComponent.DBSBuffer.Database.MySQL.NewDataset import NewDataset as MySQLNewDataset

class NewDataset(MySQLNewDataset):
    """
    _NewDataset_

    Add a new dataset to DBS Buffer
    """
    sql = """INSERT INTO dbsbuffer_dataset (path)
               SELECT :path WHERE NOT EXISTS
                 (SELECT * FROM dbsbuffer_dataset WHERE path = :path)"""
    existsSQL = "SELECT id FROM dbsbuffer_dataset WHERE path = :path"
