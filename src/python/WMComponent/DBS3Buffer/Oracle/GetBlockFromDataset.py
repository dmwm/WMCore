#!/usr/bin/env python
"""
_ListDataset_

Oracle implementation of DBSBuffer.GetBlockFromDataset
"""




from WMComponent.DBSBuffer.Database.MySQL.GetBlockFromDataset import GetBlockFromDataset as MySQLGetBlockFromDataset

class GetBlockFromDataset(MySQLGetBlockFromDataset):
    """
    _GetBlockFromDataset_

    Identical to MySQL version
    """
