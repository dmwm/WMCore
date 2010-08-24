#!/usr/bin/env python
"""
_Failed_
Oracle implementation of Jobs.Failed

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Failed.py,v 1.3 2008/12/05 21:06:25 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobMySQL


class Failed(FailedJobMySQL):
    sql = FailedJobMySQL.sql