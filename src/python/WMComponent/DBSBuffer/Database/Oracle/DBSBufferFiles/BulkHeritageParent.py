#!/usr/bin/env python
"""
_HeritageLFNParent_

Oracle implementation of DBSBufferFiles.HeritageLFNParent
"""

__revision__ = "$Id: BulkHeritageParent.py,v 1.1 2010/05/24 20:36:53 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMComponent.DBSBuffer.Database.MySQL.DBSBufferFiles.BulkHeritageParent import BulkHeritageParent as MySQLBulkHeritageParent


class BulkHeritageParent(DBFormatter):
    """
    Commit parentage information in bulk


    """
