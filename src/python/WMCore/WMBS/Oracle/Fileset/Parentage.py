#!/usr/bin/env python
"""
_Parentage_

Oracle implementation of Fileset.Parentage

"""
__all__ = []



from WMCore.WMBS.MySQL.Fileset.Parentage import Parentage as FilesetParentageMySQL

class Parentage(FilesetParentageMySQL):
    sql = FilesetParentageMySQL.sql
