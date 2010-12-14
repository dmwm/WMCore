#!/usr/bin/env python
"""
_UpdateAlgoDatasetAssoc_

SQLite implementation of DBSBuffer.UpdateAlgoDatasetAssoc
"""




from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgoDatasetAssoc import UpdateAlgoDatasetAssoc as MySQLUpdateAlgoDatasetAssoc

class UpdateAlgoDatasetAssoc(MySQLUpdateAlgoDatasetAssoc):
    """
    _UpdateAlgoDatasetAssoc_

    Update the in_dbs column for a particular algo/dataset association.
    """
    pass
