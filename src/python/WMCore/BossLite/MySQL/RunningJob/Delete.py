#!/usr/bin/env python
"""
_Delete_

MySQL implementation of BossLite.RunningJob.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.3 2010/05/12 09:49:11 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.BossLite.MySQL.Task.Delete import Delete as TaskDelete

class Delete(TaskDelete):
    """
    BossLite.RunningJob.Delete
    """
    
    sql = """DELETE FROM bl_runningjob WHERE %s """
