#!/usr/bin/env python
"""
_GetAllJobs_

Oracle implementation of Jobs.GetAllJobs
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.GetAllJobs import GetAllJobs as MySQLGetAllJobs

class GetAllJobs(MySQLGetAllJobs):
    """
    Right now the same as the MySQL version

    """
