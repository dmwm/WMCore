#!/usr/bin/env python
"""
_SetBlock

SQLite implementation of DBSBufferFiles.SetBlock
"""

__revision__ = "$Id: SetBlock.py,v 1.1 2009/09/22 19:50:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetBlock import SetBlock as MySQLSetBlock

class SetBlock(MySQLSetBlock):
    """
    _SetBlock_

    SQLite implementation of SetBlock
    """
    pass
