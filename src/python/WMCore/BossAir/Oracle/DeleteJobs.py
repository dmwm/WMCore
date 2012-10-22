#!/usr/bin/env python
"""
_DeleteJobs_

Oracle implementation for creating a deleting a job
"""


from WMCore.BossAir.MySQL.DeleteJobs import DeleteJobs as MySQLDeleteJobs

class DeleteJobs(MySQLDeleteJobs):
    """
    _DeleteJobs_

    Delete jobs from bl_runjob
    """
