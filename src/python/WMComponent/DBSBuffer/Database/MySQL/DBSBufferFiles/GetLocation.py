#!/usr/bin/env python
"""
_GetLocation_

MySQL implementation of File.GetLocation
"""

__revision__ = "$Id: GetLocation.py,v 1.3 2009/12/16 17:45:38 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetLocation(DBFormatter):
    sql = """select se_name from dbsbuffer_location 
                where id in (select location from dbsbuffer_file_location 
                    where filename in (select id from dbsbuffer_file where lfn=:lfn))"""
                    
    
    def getBinds(self, file=None):
        binds = []
        file = self.dbi.makelist(file)
        for f in file:
            binds.append({'lfn': f})
        return binds
    
    def format(self, result):
        "Return a list of SE FQDN's"
        out = set()
        for r in result:
            for i in r.fetchall():
                out.add(i[0])
        return out
    
    def execute(self, file=None, conn = None, transaction = False):
        binds = self.getBinds(file)
        
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
