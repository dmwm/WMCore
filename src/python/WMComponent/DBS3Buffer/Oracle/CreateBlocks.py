#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""




import threading
import exceptions

from WMComponent.DBS3Buffer.MySQL.CreateBlocks import CreateBlocks as MySQLCreateBlocks

class CreateBlocks(MySQLCreateBlocks):
    """
    Oracle implementation

    """
