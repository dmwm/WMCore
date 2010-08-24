#!/usr/bin/env python
"""
_LoadDBSFilesByDAS_

Oracle implementation of LoadDBSFilesByDAS
"""




import logging

from WMComponent.DBSUpload.Database.MySQL.LoadDBSFilesByDAS import LoadDBSFilesByDAS as MySQLLoadDBSFilesByDAS

class LoadDBSFilesByDAS(MySQLLoadDBSFilesByDAS):
    """
    _LoadDBSFilesByDAS_

    Oracle implementation, untested
    """
