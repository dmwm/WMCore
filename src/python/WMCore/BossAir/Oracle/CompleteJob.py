#!/usr/bin/env python
"""
_CompleteJob_

Oracle implementation for labeling a job Complete
"""


from WMCore.BossAir.MySQL.CompleteJob import CompleteJob as MySQLCompleteJob

class CompleteJob(MySQLCompleteJob):
    """
    _CompleteJob_

    Label jobs as complete
    """
