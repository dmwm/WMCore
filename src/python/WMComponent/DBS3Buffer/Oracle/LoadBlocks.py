#!/usr/bin/env python
"""
_LoadBlocks_

Oracle implementation of DBS3Buffer.LoadBlocks
"""

from WMComponent.DBSBuffer.Database.MySQL.LoadBlocks import LoadBlocks as MySQLLoadBlocks

class LoadBlocks(MySQLLoadBlocks):
    """
    Identical to MySQL version.
    """
