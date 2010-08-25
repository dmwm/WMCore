#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.Job.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2010/05/12 09:49:10 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    """
    BossLite.Job.Delete
    """
    
    sql = """DELETE FROM bl_job WHERE %s """
