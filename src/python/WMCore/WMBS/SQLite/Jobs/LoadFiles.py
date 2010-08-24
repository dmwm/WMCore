#!/usr/bin/env python
"""
_LoadFiles_

SQLite implementation of Jobs.LoadFiles
"""

__all__ = []
__revision__ = "$Id: LoadFiles.py,v 1.1 2008/11/21 17:14:14 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Jobs.LoadFiles import LoadFiles as LoadFilesMySQL

class LoadFiles(LoadFilesMySQL):
    sql = LoadFilesMySQL.sql
