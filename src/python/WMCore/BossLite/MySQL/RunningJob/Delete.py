#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.RunningJob.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.2 2010/05/10 13:00:10 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    """
    BossLite.RunningJob.Delete
    """
    
    sql = """DELETE FROM bl_runningjob WHERE %s = :value"""
