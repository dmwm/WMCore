#!/usr/bin/env python
"""
_DeleteCheckWorkflow_

SQLite implementation of DeleteCheckWorkflow

"""
__all__ = []
__revision__ = "$Id: DeleteCheck.py,v 1.1 2009/09/25 15:14:03 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Workflow.DeleteCheck import DeleteCheck as MySQLDeleteCheck

class DeleteCheck(MySQLDeleteCheck):
    """
    Same as MySQL version

    """
