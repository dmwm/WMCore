#!/usr/bin/env python
"""
_DeleteParentCheck_

Oracle implementation of DeleteParentCheck

"""
__all__ = []
__revision__ = "$Id: DeleteParentCheck.py,v 1.1 2010/04/07 20:29:31 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.DeleteParentCheck import DeleteParentCheck as MySQLDeleteParentCheck

class DeleteParentCheck(MySQLDeleteParentCheck):
    """
    Same as MySQL

    """
