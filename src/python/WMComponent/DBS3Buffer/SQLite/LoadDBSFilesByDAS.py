#!/usr/bin/env python
"""
_LoadDBSFilesByDAS_

SQLite implementation of LoadDBSFilesByDAS
"""




import logging

from WMComponent.DBSUpload.Database.MySQL.LoadDBSFilesByDAS import LoadDBSFilesByDAS as MySQLLoadDBSFilesByDAS

class LoadDBSFilesByDAS(MySQLLoadDBSFilesByDAS):
    """
    _LoadDBSFilesByDAS_

    SQLite implementation, untested
    """
