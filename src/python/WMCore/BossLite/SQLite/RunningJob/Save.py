#!/usr/bin/env python
"""
_Save_

SQLite implementation of BossLite.RunningJob.Save
"""

__all__ = []
__revision__ = "$Id: Save.py,v 1.1 2010/03/30 10:22:59 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.BossLite.MySQL.RunningJob.Save import Save as MySQLSave

class Save(MySQLSave):
    """
    Identical to MySQL

    """
