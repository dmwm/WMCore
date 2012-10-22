#!/usr/bin/env python
"""
_UpdateJobs_

Oracle implementation for updating jobs
"""


from WMCore.BossAir.MySQL.UpdateJobs import UpdateJobs as MySQLUpdateJobs

class UpdateJobs(MySQLUpdateJobs):
    """
    _UpdateJobs_

    Update jobs with new values
    """
