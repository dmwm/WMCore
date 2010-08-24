#!/usr/bin/env python
"""
_LoadIDFromUID_

SQLite implementation of JobGroup.LoadIDFromUID
"""

__all__ = []
__revision__ = "$Id: LoadIDFromUID.py,v 1.1 2009/01/06 15:54:50 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadIDFromUID import LoadIDFromUID as LoadIDFromUIDMySQL

class LoadIDFromUID(LoadIDFromUIDMySQL):
    sql = LoadIDFromUIDMySQL.sql
