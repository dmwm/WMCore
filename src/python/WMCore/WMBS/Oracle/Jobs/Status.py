#!/usr/bin/env python
"""
_New_
Oracle implementation of Jobs.Status
"""

__all__ = []
__revision__ = "$Id: Status.py,v 1.1 2009/04/29 16:26:48 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.Status import Status as StatusJobsMySQL

class Status(StatusJobsMySQL):
    sql = StatusJobsMySQL.sql
