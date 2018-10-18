"""
_IsAllWorkflowsCompleted_

Oracle implementation of DBS3Buffer.IsAllWorkflowsCompleted
"""

from __future__ import print_function, division

from WMComponent.DBS3Buffer.MySQL.IsAllWorkflowsCompleted \
    import IsAllWorkflowsCompleted as MySQLIsAllWorkflowsCompleted

class IsAllWorkflowsCompleted(MySQLIsAllWorkflowsCompleted):
    """
    Oracle version
    """
    pass
