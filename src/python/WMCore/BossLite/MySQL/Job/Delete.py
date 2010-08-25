#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.Job.Delete
"""

__all__ = []



from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    """
    BossLite.Job.Delete
    """
    
    sql = """DELETE FROM bl_job WHERE %s """
