#!/usr/bin/env python
"""
_ListDataset_

SQLite implementation of DBSBuffer.GetBlockFromDataset
"""

__revision__ = "$Id: GetBlockFromDataset.py,v 1.1 2009/12/02 20:09:36 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.GetBlockFromDataset import GetBlockFromDataset as MySQLGetBlockFromDataset

class GetBlockFromDataset(MySQLGetBlockFromDataset):
    """
    _GetBlockFromDataset_

    Identical to MySQL version
    """
