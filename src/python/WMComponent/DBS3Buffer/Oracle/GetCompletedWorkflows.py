#!/usr/bin/env python
"""
_GetCompletedWorkflows_

Oracle implementation of DBS3Buffer.GetCompletedWorkflows
"""




from WMComponent.DBS3Buffer.MySQL.GetCompletedWorkflows \
    import GetCompletedWorkflows as MySQLGetCompletedWorkflows

class GetCompletedWorkflows(MySQLGetCompletedWorkflows):
    """
    Oracle version
    """
