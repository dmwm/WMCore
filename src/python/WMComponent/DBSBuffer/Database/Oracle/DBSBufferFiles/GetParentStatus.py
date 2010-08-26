#!/usr/bin/env python
"""
_GetParentStatus_

Oracle implementation of DBSBufferFile.GetParentStatus
"""

__revision__ = "$Id: GetParentStatus.py,v 1.1 2010/01/13 19:54:44 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.GetParentStatus import GetParentStatus as MySQLGetParentStatus

class GetParentStatus(MySQLGetParentStatus):
    pass
