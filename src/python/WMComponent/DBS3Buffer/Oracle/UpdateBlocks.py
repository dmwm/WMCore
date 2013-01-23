#!/usr/bin/env python
"""
_UpdateBlocks_

Oracle implementation of DBS3Buffer.UpdateBlocks
"""

from WMComponent.DBSBuffer.Database.MySQL.UpdateBlocks import UpdateBlocks as MySQLUpdateBlocks

class UpdateBlocks(MySQLUpdateBlocks):
    """
    Identical to MySQL version.
    """
