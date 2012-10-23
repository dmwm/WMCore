#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Jobs.Exists
"""

__all__ = []



from WMCore.WMBS.MySQL.Jobs.Exists import Exists as ExistsJobMySQL

class Exists(ExistsJobMySQL):
    sql = ExistsJobMySQL.sql
