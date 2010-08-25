#!/usr/bin/env python
"""
_AlgoDatasetAssoc_

Associate an algorithm with a dataset in DBSBuffer.
"""

__revision__ = "$Id: AlgoDatasetAssoc.py,v 1.1 2009/07/14 19:20:42 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.AlgoDatasetAssoc import AlgoDatasetAssoc as MySQLAlgoDatasetAssoc

class AlgoDatasetAssoc(MySQLAlgoDatasetAssoc):
    """
    _AlgoDatasetAssoc_

    Associate an algorithm with a dataset in DBSBuffer.
    """
    pass
