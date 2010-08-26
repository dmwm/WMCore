#!/usr/bin/env python
"""
_UpdateAlgoDatasetAssoc_

Oracle implementation of DBSBuffer.UpdateAlgoDatasetAssoc
"""

__revision__ = "$Id: UpdateAlgoDatasetAssoc.py,v 1.1 2009/07/14 19:20:42 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.UpdateAlgoDatasetAssoc import UpdateAlgoDatasetAssoc as MySQLUpdateAlgoDatasetAssoc

class UpdateAlgoDatasetAssoc(MySQLUpdateAlgoDatasetAssoc):
    """
    _UpdateAlgoDatasetAssoc_

    Update the in_dbs column for a particular algo/dataset association.
    """
    pass
