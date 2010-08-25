#!/usr/bin/env python
"""
_GetBlock_

SQLite implementation of DBSBufferFiles.GetBlock
"""

__revision__ = "$Id: GetBlock.py,v 1.1 2009/09/22 19:50:35 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetBlock import GetBlock as MySQLGetBlock

class GetBlock(MySQLGetBlock):
    """
    _GetBlock_
    
    SQLite implementation of DBSBufferFiles.GetBlock
    """
    pass
