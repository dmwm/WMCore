#!/usr/bin/env python
"""
_Status_

SQLite implementation of DBSBuffer.Status
"""

__revision__ = "$Id: Status.py,v 1.1 2010/05/26 21:04:20 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.Status import Status as MySQLStatus

class Status(MySQLStatus):
    pass
