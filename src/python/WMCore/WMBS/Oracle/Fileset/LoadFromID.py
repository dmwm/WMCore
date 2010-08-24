#!/usr/bin/env python
"""
_Load_

Oracle implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.2 2008/11/24 21:51:53 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.LoadFromID import LoadFromID as LoadFilesetMySQL

class LoadFromID(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = self.truefalse(result[1])
        name = result[0]
        return name, open, time