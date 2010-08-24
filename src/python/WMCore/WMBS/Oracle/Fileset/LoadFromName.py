#!/usr/bin/env python
"""
_Load_

Oracle implementation of LoadFileset

"""
__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.3 2008/12/05 21:06:26 sryu Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Fileset.LoadFromName import LoadFromName as LoadFilesetMySQL

class LoadFromName(LoadFilesetMySQL):
    sql = LoadFilesetMySQL.sql
    
    def format(self, result):
        result = result[0].fetchall()[0]
        time = result[2]
        open = self.truefalse(result[1])
        id = int(result[0])
        return id, open, time