#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Subscription.Exists

TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/11/20 16:57:49 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = """select id from wmbs_subscription
             where fileset = :fileset and workflow = :workflow and type = :type"""
    
    def format(self, result):
        result = MySQLBase.format(self, result)

        if len(result) > 0:
            return result[0][0]
        else:
            return False
        
    def execute(self, workflow=None, fileset=None, type = None, 
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, self.getBinds(workflow=workflow, fileset=fileset, type=type), 
                         conn = conn, transaction = transaction)
        return self.format(result)
