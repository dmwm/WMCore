#!/usr/bin/env python
"""
_FailedJobs_

Oracle implementation of Workflow.FailedJobs
"""

__revision__ = "$Id: FailedJobs.py,v 1.1 2010/06/14 20:38:42 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.FailedJobs import FailedJobs as MySQLFailedJobs

class FailedJobs(MySQLFailedJobs):
    pass
    
