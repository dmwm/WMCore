#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Fileset.Exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/10/08 14:30:11 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Fileset.Exists import Exists as ExistsFilesetMySQL

class Exists(ExistsFilesetMySQL, SQLiteBase):
    sql = ExistsFilesetMySQL.sql