#!/usr/bin/env python
"""
_FailedJobsByWorkflow_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.FailedJobsByWorkflow \
  import FailedJobsByWorkflow as FailedJobsByWorkflowMySQL

class FailedJobsByWorkflow(FailedJobsByWorkflowMySQL):
    pass
