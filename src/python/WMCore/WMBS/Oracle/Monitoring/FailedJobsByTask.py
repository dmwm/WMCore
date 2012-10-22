#!/usr/bin/env python
"""
_FailedJobsByTask_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.FailedJobsByTask \
  import FailedJobsByTask as FailedJobsByTaskMySQL

class FailedJobsByTask(FailedJobsByTaskMySQL):
    pass
