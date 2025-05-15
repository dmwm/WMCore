#!/usr/bin/env python
"""
_Parentage_

Oracle implementation of Fileset.New

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.New import New as NewFilesetMySQL

class New(NewFilesetMySQL):
    sql = """INSERT INTO wmbs_fileset (name, last_update, open)
             VALUES (:NAME, :LAST_UPDATE, :OPEN)"""
