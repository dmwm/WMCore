from __future__ import print_function, division
"""
_IsAllWorkflowsCompleted_

Oracle implementation of DBS3Buffer.IsAllWorkflowsCompleted
"""




from WMComponent.DBS3Buffer.MySQL.IsAllWorkflowsCompleted \
    import IsAllWorkflowsCompleted as MySQLIsAllWorkflowsCompleted

class IsAllWorkflowsCompleted(MySQLIsAllWorkflowsCompleted):
    """
    Oracle version
    """
    pass