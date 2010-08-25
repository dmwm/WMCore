#!/usr/bin/env python
"""
_SucceededJobs_

Oracle implementation of Subscriptions.SucceededJobs
"""

__revision__ = "$Id: SucceededJobs.py,v 1.2 2010/06/28 19:05:00 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.SucceededJobs import SucceededJobs as SucceededJobsMySQL

class SucceededJobs(SucceededJobsMySQL):
    pass


