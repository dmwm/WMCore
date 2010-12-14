#!/usr/bin/env python
"""
_GetOpenBlocks_

Oracle implementation of DBSBuffer.GetOpenBlocks
"""




from WMComponent.DBSBuffer.Database.MySQL.GetOpenBlocks import GetOpenBlocks as MySQLGetOpenBlocks

class GetOpenBlocks(MySQLGetOpenBlocks):
    """
    Identical to MySQL version

    """
