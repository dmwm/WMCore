#!/usr/bin/env python
"""
_UpdateName_
SQLite implementation of Jobs.UpdateName

Delete all status information. For resubmissions and for each state change.
"""
__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.1 2008/10/17 13:22:50 jcgon Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.UpdateName import UpdateName as UpdateNameJobsMySQL
#from WMCore.Database.DBFormatter import DBFormatter

class UpdateName(UpdateNameJobsMySQL):
    sql=UpdateNameJobsMySQL.sql
