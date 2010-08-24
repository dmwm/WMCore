#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Files.Exists

"""

__all__ = []
__revision__ = "$Id: Exists.py,v 1.1 2008/10/22 19:08:28 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Files.Exists import Exists as FilesExistsMySQL

class Exists(FilesExistsMySQL, SQLiteBase):
    sql = FilesExistsMySQL.sql
