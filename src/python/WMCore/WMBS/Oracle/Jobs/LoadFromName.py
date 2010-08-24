#!/usr/bin/env python
"""
_LoadFromName_

Oracle implementation of Jobs.LoadFromName.
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2009/01/13 17:39:55 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromName import LoadFromName as LoadFromNameJobMySQL

class LoadFromName(LoadFromNameJobMySQL):
    sql = LoadFromNameJobMySQL.sql
