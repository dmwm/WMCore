#!/usr/bin/env python
"""
_LoadFromUID_

SQLite implementation of JobGroup.LoadFromUID
"""

__all__ = []
__revision__ = "$Id: LoadFromUID.py,v 1.1 2009/01/14 16:35:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadFromUID import LoadFromUID as LoadFromUIDMySQL

class LoadFromUID(LoadFromUIDMySQL):
    sql = LoadFromUIDMySQL.sql
