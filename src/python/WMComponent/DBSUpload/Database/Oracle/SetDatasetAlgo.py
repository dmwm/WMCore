#!/usr/bin/env python
"""
_SetDatasetAlgo_

Oracle implementation of DBSUpload.SetDatabaseAlgo
Should set the database-algo inDBS switch
"""

__revision__ = "$Id: SetDatasetAlgo.py,v 1.1 2010/02/24 21:41:34 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSUpload.Database.MySQL.SetDatasetAlgo import SetDatasetAlgo as MySQLSetDatasetAlgo

class SetDatasetAlgo(MySQLSetDatasetAlgo):
    """
    Identical to MySQL version

    """

