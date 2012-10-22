#!/usr/bin/env python
"""
_SetLastUpdate_

Oracle implementation of SetLastUpdateFileset

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.SetLastUpdate import SetLastUpdate as SetLastUpdateFilesetMySQL

class SetLastUpdate(SetLastUpdateFilesetMySQL):
    sql = SetLastUpdateFilesetMySQL.sql
