#!/usr/bin/env python
"""
_LoadFromName_

SQLite implementation of Jobs.LoadFromName
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.1 2008/11/21 17:14:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameMySQL

class LoadFromName(LoadFromNameMySQL):
    sql = LoadFromNameMySQL.sql
