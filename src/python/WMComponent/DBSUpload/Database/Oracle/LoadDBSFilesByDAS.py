#!/usr/bin/env python
"""
_LoadDBSFilesByDAS_

Oracle implementation of LoadDBSFilesByDAS
"""

__revision__ = "$Id: LoadDBSFilesByDAS.py,v 1.1 2010/06/04 18:58:27 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

import logging

from WMComponent.DBSUpload.Database.MySQL.LoadDBSFilesByDAS import LoadDBSFilesByDAS as MySQLLoadDBSFilesByDAS

class LoadDBSFilesByDAS(MySQLLoadDBSFilesByDAS):
    """
    _LoadDBSFilesByDAS_

    Oracle implementation, untested
    """
