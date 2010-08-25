#!/usr/bin/env python
"""
_DBSBuffer.SetBlockStatus_

Create new block in dbsbuffer_block
Update file to reflect block information
"""


__revision__ = "$Id: SetBlockStatus.py,v 1.2 2009/09/03 18:56:18 mnorman Exp $"
__version__ = "$Revision: 1.2 $"


import threading
import exceptions

from WMComponent.DBSUpload.Database.MySQL.SetBlockStatus import SetBlockStatus as MySQLSetBlockStatus


class SetBlockStatus(MySQLSetBlockStatus):
    """
    Oracle implementation

    """
