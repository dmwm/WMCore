#!/usr/bin/env python
"""
_Exists_

Oracle implementation of JobGroup.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.Exists import Exists as ExistsJobGroupMySQL

class Exists(ExistsJobGroupMySQL):
    pass
