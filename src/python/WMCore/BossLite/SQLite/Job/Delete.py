#!/usr/bin/env python
"""
_Delete_

SQLite implementation of BossLite.Job.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2010/04/19 17:57:21 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Job.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    Delete some files.

    """
