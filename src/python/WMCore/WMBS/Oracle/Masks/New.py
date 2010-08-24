#!/usr/bin/env python
"""
_New_

Oracle implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/01/11 17:49:37 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Masks.New import New as NewMasksMySQL

class New(NewMasksMySQL):
    sql = NewMasksMySQL.sql
    
    def execute(self, jobid, inclusivemask = None, conn = None,
                transaction = False):
        if inclusivemask == None or inclusivemask:
            # default value
            inclusivemask = 'Y'
        else:
            inclusivemask = 'N'
                
        binds = self.getBinds(jobid = jobid, inclusivemask = inclusivemask)
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
