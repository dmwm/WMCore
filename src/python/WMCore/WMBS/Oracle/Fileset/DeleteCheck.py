#!/usr/bin/env python
"""
_DeleteCheckFileset_

SQLite implementation of DeleteCheckFileset

"""
__all__ = []
__revision__ = "$Id: DeleteCheck.py,v 1.1 2009/09/25 15:14:55 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.DeleteCheck import DeleteCheck as MySQLDeleteCheck

class DeleteCheck(MySQLDeleteCheck):
    """
    Same as MySQL version

    """
