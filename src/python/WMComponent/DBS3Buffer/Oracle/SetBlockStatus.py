#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""






import threading
import exceptions

from WMComponent.DBSUpload.Database.MySQL.SetBlockStatus import SetBlockStatus as MySQLSetBlockStatus


class SetBlockStatus(MySQLSetBlockStatus):
    """
    Oracle implementation

    """
