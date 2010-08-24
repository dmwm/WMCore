#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.RunningJob.Delete
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    """
    BossLite.RunningJob.Delete
    """
    
    sql = """DELETE FROM bl_runningjob WHERE %s """
