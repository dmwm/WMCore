#!/usr/bin/env python
"""
_Load_

MySQL implementation of Subscription.Load
            
TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
"""

__all__ = []
__revision__ = "$Id: Load.py,v 1.5 2008/11/20 21:52:32 sryu Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Load(DBFormatter):
    
    def getSQL(self, **kwargs):
        sql = ""
        binds = {}
        if kwargs['id'] != -1:
            sql = """select id, fileset, workflow, type, split_algo from wmbs_subscription 
            where id = :id"""
            binds = self.getBinds(id=kwargs['id'])
        else:
            sql = """select id, fileset, workflow, type, split_algo from wmbs_subscription 
            where fileset = :fileset and workflow = :workflow and type = :type"""
            binds = self.getBinds(fileset=int(kwargs['fileset']), 
                                  workflow=int(kwargs['workflow']),
                                  type=kwargs['type'])
        return sql, binds
    
    def format(self,result):
        """
        TODO: return id, fileset, workflow, type as a dictionary
        """
        result = result[0].fetchall()[0]
        result = {'id': int(result[0]), 
                  'fileset': int(result[1]), 
                  'workflow': int(result[2]), 
                  'type': result[3],
                  'split_algo': result[4]}

        return result
        
    def execute(self, id = None, workflow = None, type = None, fileset = None, 
                conn = None, transaction = False):
        sql, binds = self.getSQL(id=id, workflow=workflow, type=type, fileset=fileset)
        result = self.dbi.processData(sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)
