#!/usr/bin/env python
"""
_Save_

SQLite implementation of BossLite.Task.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2010/03/30 10:19:34 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.Task.Save import Save as MySQLSave

class Save(MySQLSave):
    """
    Identical to MySQL

    """
