#!/usr/bin/env python
"""
_UpdateName_
SQLite implementation of Jobs.UpdateName

Delete all status information. For resubmissions and for each state change.
"""

__all__ = []
__revision__ = "$Id: UpdateName.py,v 1.2 2009/01/12 19:26:06 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.UpdateName import UpdateName as UpdateNameJobsMySQL

class UpdateName(UpdateNameJobsMySQL):
    sql=UpdateNameJobsMySQL.sql
