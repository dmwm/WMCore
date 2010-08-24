#!/usr/bin/env python
"""
_Failed_
MySQL implementation of Jobs.Failed

move file into wmbs_group_job_acquired
"""
__all__ = []
__revision__ = "$Id: Failed.py,v 1.2 2008/11/24 21:51:38 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobMySQL


class Failed(FailedJobMySQL):
    sql = FailedJobMySQL.sql