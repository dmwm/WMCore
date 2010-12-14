#!/usr/bin/env python
"""
_SetDatasetAlgo_

Oracle implementation of DBSUpload.SetDatabaseAlgo
Should set the database-algo inDBS switch
"""




from WMComponent.DBSUpload.Database.MySQL.SetDatasetAlgo import SetDatasetAlgo as MySQLSetDatasetAlgo

class SetDatasetAlgo(MySQLSetDatasetAlgo):
    """
    Identical to MySQL version

    """

