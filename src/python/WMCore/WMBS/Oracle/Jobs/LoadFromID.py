#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Jobs.LoadFromID.
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.3 2009/01/13 17:39:55 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Jobs.LoadFromID import LoadFromID as LoadFromIDJobMySQL

class LoadFromID(LoadFromIDJobMySQL):
    sql = LoadFromIDJobMySQL.sql
