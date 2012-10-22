#!/usr/bin/env python
"""
_Exists_

Oracle implementation of Fileset.Exists

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.Exists import Exists as ExistsFilesetMySQL

class Exists(ExistsFilesetMySQL):
    sql = ExistsFilesetMySQL.sql
