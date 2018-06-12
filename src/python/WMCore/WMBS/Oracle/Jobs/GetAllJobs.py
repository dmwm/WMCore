#!/usr/bin/env python
"""
_GetAllJobs_

Oracle implementation of Jobs.GetAllJobs
"""

from WMCore.WMBS.MySQL.Jobs.GetAllJobs import GetAllJobs as MySQLGetAllJobs


class GetAllJobs(MySQLGetAllJobs):
    """
    Besides the row limitation, it's the same as in MySQL
    """
    limit_sql = " AND ROWNUM <= %d"
