#!/usr/bin/env python
"""
_SetStatus_

SQLite implementation of DBSBufferFiles.SetStatus
"""

__revision__ = "$Id: SetStatus.py,v 1.1 2009/09/25 13:25:36 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.SetStatus import SetStatus as MySQLSetStatus

class SetStatus(MySQLSetStatus):
    """
    _SetStatus_

    SQLite implementation of SetStatus
    """
    pass
