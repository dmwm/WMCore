#!/usr/bin/env python
"""
_UpdateBlocks_

Oracle implementation of DBS3Buffer.UpdateBlocks
"""

from WMComponent.DBS3Buffer.MySQL.UpdateWorkflowsToCompleted \
     import UpdateWorkflowsToCompleted as MySQLUpdateWorkflowsToCompleted

class UpdateWorkflowsToCompleted(MySQLUpdateWorkflowsToCompleted):
    """
    Identical to MySQL version.
    """
