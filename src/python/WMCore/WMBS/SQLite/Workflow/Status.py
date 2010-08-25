#!/usr/bin/env python
"""
_Status_

SQLite implementation of Workflow.Status
"""

__revision__ = "$Id: Status.py,v 1.1 2010/05/26 21:04:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.Status import Status as MySQLStatus

class Status(MySQLStatus):
    pass
    
