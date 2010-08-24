#!/usr/bin/env python
"""
_Parentage_

Oracle implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/24 21:51:53 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.New import New as NewFilesetMySQL

class New(NewFilesetMySQL):
    sql = """insert into wmbs_fileset 
            (id, name, open, last_update) values (wmbs_fileset_SEQ.nextval, 
                                                   :fileset, :open, :timestamp)"""
            
    def getBinds(self, name = None, open=0):
        binds = self.dbi.buildbinds(self.dbi.makelist(name), 'fileset',
                  self.dbi.buildbinds(self.dbi.makelist(open), 
                                      'open', 
                    self.dbi.buildbinds(
                        self.dbi.makelist(self.timestamp()), 'timestamp')))
        return binds