#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of LoadFileset
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.4 2009/01/13 16:38:53 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.MySQL.Fileset.LoadFromID import LoadFromID as LoadFilesetMySQL

class LoadFromID(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
