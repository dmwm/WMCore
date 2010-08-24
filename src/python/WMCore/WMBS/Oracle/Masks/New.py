#!/usr/bin/env python
"""
_New_

Oracle implementation of Masks.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/12/05 21:06:24 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Masks.New import New as NewMasksMySQL

class New(NewMasksMySQL):
    
    def execute(self, jobid, inclusivemask=None):
        if inclusivemask == None or inclusivemask:
            # default value
            inclusivemask = 'Y'
        else:
            inclusivemask = 'N'
                
        binds = self.getBinds(jobid = jobid, inclusivemask = inclusivemask)
        result = self.dbi.processData(self.sql, binds)
        return self.format(result)