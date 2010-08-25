#!/usr/bin/env python
"""
_SetLastUpdate_

SQLite implementation of SetLastUpdateFileset

"""
__all__ = []
__revision__ = "$Id: SetLastUpdate.py,v 1.1 2010/05/11 20:56:37 riahi Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.SetLastUpdate import SetLastUpdate as SetLastUpdateFilesetMySQL

class SetLastUpdate(SetLastUpdateFilesetMySQL):
    sql = SetLastUpdateFilesetMySQL.sql

