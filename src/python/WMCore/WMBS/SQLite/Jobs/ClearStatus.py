#!/usr/bin/env python
"""
_ClearStatus_
SQLite implementation of Jobs.ClearStatus

Delete all status information. For resubmissions and for each state change.
"""

__all__ = []
__revision__ = "$Id: ClearStatus.py,v 1.2 2009/01/12 19:26:06 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.ClearStatus import ClearStatus as ClearStatusJobsMySQL

class ClearStatus(ClearStatusJobsMySQL):
    sql=ClearStatusJobsMySQL.sql
