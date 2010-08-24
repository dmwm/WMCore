#!/usr/bin/env python
"""
_New_
Oracle implementation of JobGroup.Status
"""

__all__ = []
__revision__ = "$Id: Status.py,v 1.4 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.JobGroup.Status import Status as StatusJobGroupMySQL

class Status(StatusJobGroupMySQL):
    sql = StatusJobGroupMySQL.sql
