#!/usr/bin/env python
"""
_RetriedJobs_

SQLite implementation of Workflow.FailedJobs
"""

__revision__ = "$Id: RetriedJobs.py,v 1.1 2010/06/17 14:22:43 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.RetriedJobs import RetriedJobs as MySQLRetriedJobs

class RetriedJobs(MySQLRetriedJobs):
    pass
    
