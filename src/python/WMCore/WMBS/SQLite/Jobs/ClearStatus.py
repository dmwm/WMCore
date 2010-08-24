#!/usr/bin/env python
"""
_ClearStatus_
SQLite implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.1 2008/10/17 13:22:50 jcgon Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.ClearStatus import ClearStatus as ClearStatusJobsMySQL
#from WMCore.Database.DBFormatter import DBFormatter

class ClearStatus(ClearStatusJobsMySQL):
    sql=ClearStatusJobsMySQL.sql
