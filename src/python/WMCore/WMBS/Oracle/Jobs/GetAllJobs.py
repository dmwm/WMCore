#!/usr/bin/env python
"""
_GetAllJobs_

Oracle implementation of Jobs.GetAllJobs
"""

__all__ = []
__revision__ = "$Id: GetAllJobs.py,v 1.1 2009/07/30 19:29:43 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.GetAllJobs import GetAllJobs as MySQLGetAllJobs

class GetAllJobs(MySQLGetAllJobs):
    """
    Right now the same as the MySQL version

    """
