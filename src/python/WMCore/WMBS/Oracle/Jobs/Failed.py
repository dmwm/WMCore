#!/usr/bin/env python
"""
_Failed_
Oracle implementation of Jobs.Failed

move file into wmbs_group_job_acquired
"""

__all__ = []
__revision__ = "$Id: Failed.py,v 1.4 2009/01/12 19:26:04 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobMySQL

class Failed(FailedJobMySQL):
    sql = FailedJobMySQL.sql
