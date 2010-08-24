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
__revision__ = "$Id: Load.py,v 1.1 2008/06/23 09:36:17 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Load(MySQLBase):
    
    def getSQL(self, **kwargs):
        sql = ""
        binds = {}
        if kwargs['id']:
            sql = """select id, fileset, workflow, type from wmbs_subscription 
            where id = :id"""
            binds = self.getBinds({'id':kwargs['id']})
        else:
            sql = """select id, fileset, workflow, type from wmbs_subscription 
            where fileset = :fileset, workflow=:workflow, type=:type"""
            binds = self.getBinds({'fileset':kwargs['fileset'], 'workflow':kwargs['workflow']})
        return sql, binds
    
    def format(self,result):
        """
        TODO: return id, fileset, workflow, type as a dictionary
        """
        result = MySQLBase.format(result)
        
    def execute(self, id = None, workflow = None, type = None, fileset = None, 
                conn = None, transaction = False):
        sql, binds = getSQL(id=id, workflow=workflow, type=type, fileset=fileset)
        
        result = self.dbi.processData(self.sql, self.getBinds(fileset), 
                         conn = conn, transaction = transaction)
        return self.format(result)