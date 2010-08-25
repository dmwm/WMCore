#!/usr/bin/env python
"""
_UpdateAlgoDatasetAssoc_

SQLite implementation of DBSBuffer.UpdateAlgoDatasetAssoc
"""

__revision__ = "$Id: UpdateAlgoDatasetAssoc.py,v 1.1 2009/07/13 19:44:27 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgoDatasetAssoc import UpdateAlgoDatasetAssoc as MySQLUpdateAlgoDatasetAssoc

class UpdateAlgoDatasetAssoc(MySQLUpdateAlgoDatasetAssoc):
    """
    _UpdateAlgoDatasetAssoc_

    Update the in_dbs column for a particular algo/dataset association.
    """
    pass
