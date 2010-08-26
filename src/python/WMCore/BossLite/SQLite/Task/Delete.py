#!/usr/bin/env python
"""
_Delete_

SQLite implementation of BossLite.Task.Delete
"""

__all__ = []
__revision__ = "$Id: Delete.py,v 1.1 2010/04/19 17:57:22 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.Delete import Delete as MySQLDelete

class Delete(MySQLDelete):
    """
    Delete some files.

    """
