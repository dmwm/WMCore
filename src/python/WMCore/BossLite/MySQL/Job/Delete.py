#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.Job.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2010/04/19 17:57:21 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    sql = """DELETE FROM bl_job WHERE %s = :value"""
