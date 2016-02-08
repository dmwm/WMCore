#!/usr/bin/env python
"""
_ListAlgoDatasetAssoc_

Oracle implementation of DBSBuffer.ListAlgoDatasetAssoc
"""




from WMComponent.DBS3Buffer.MySQL.ListAlgoDatasetAssoc import ListAlgoDatasetAssoc as MySQLListAlgoDatasetAssoc

class ListAlgoDatasetAssoc(MySQLListAlgoDatasetAssoc):
    """
    _ListAlgoDatasetAssoc_

    Retrieve information about a dataset/algorthim association in the DBSBuffer.
    This is mostly used by the unit tests.
    """
    pass
