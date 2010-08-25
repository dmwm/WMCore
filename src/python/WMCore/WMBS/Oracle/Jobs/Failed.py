#!/usr/bin/env python
"""
_Failed_

Oracle implementation of Jobs.Failed
"""

__all__ = []
__revision__ = "$Id: Failed.py,v 1.6 2009/04/27 21:12:33 sryu Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobsMySQL

class Failed(FailedJobsMySQL):
    insertSQL = FailedJobsMySQL.insertSQL
    updateSQL = FailedJobsMySQL.updateSQL
