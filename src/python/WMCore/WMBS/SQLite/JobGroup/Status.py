#!/usr/bin/env python
"""
_Status_

SQLite implementation of JobGroup.Status
"""

__all__ = []
__revision__ = "$Id: Status.py,v 1.3 2008/11/21 17:11:25 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.JobGroup.Status import Status as StatusMySQL

class Status(StatusMySQL):
    sql = StatusMySQL.sql
