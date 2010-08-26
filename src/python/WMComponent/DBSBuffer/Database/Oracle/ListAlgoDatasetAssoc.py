#!/usr/bin/env python
"""
_ListAlgoDatasetAssoc_

Oracle implementation of DBSBuffer.ListAlgoDatasetAssoc
"""

__revision__ = "$Id: ListAlgoDatasetAssoc.py,v 1.1 2009/07/14 19:20:42 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.ListAlgoDatasetAssoc import ListAlgoDatasetAssoc as MySQLListAlgoDatasetAssoc

class ListAlgoDatasetAssoc(MySQLListAlgoDatasetAssoc):
    """
    _ListAlgoDatasetAssoc_

    Retrieve information about a dataset/algorthim association in the DBSBuffer.
    This is mostly used by the unit tests.
    """
    pass
