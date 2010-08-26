#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Jobs.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2008/11/21 17:14:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = LoadFromIDMySQL.sql
