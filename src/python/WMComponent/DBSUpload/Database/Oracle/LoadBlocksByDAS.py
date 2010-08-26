#!/usr/bin/env python
"""
_LoadBlocksByDAS_

Oracle implementation of LoadBlocksByDAS
"""

__revision__ = "$Id: LoadBlocksByDAS.py,v 1.1 2010/06/04 19:00:35 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSUpload.Database.MySQL.LoadBlocksByDAS import LoadBlocksByDAS as MySQLLoadBlocksByDAS

class LoadBlocksByDAS(MySQLLoadBlocksByDAS):
    """
    _LoadBlocksByDAS_

    Untested; same as MySQL
    """
