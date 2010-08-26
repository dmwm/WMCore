#!/usr/bin/env python
"""
_InsertThreshold_

SQLite implementation of ResourceControl.InsertThreshold
"""

__revision__ = "$Id: InsertThreshold.py,v 1.2 2010/02/09 17:59:15 sfoulkes Exp $"
__version__  = "$Revision: 1.2 $"

from WMCore.ResourceControl.MySQL.InsertThreshold import InsertThreshold as MySQLInsertThreshold

class InsertThreshold(MySQLInsertThreshold):
    pass
