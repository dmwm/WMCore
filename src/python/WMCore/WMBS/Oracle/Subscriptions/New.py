#!/usr/bin/env python
"""
_Subscription.New_

Oracle implementation of Subscription.New

TABLE wmbs_subscription
    id      INT(11) NOT NUNLL,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    subtype   CKECK subtypecheck in  ("merge", "processing")
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    """
    Create a workflow ready for subscriptions
    type is PL/SQL key words in Oracle
    """
    sql = """insert into wmbs_subscription 
                (id, fileset, workflow, subtype, last_update, split_algo) 
                values (wmbs_subscription_SEQ.nextval, :fileset, :workflow, 
                        (SELECT id FROM wmbs_subs_type WHERE name=:subtype), 
                        :timestamp, :split_algo)"""
        
    def execute(self, fileset = None, workflow = None, 
                split = 'File', type = 'Processing',
                spec = None, owner = None, conn = None, transaction = False):
        
        binds = self.getBinds(fileset = fileset, 
                              workflow = workflow,
                              timestamp = self.timestamp(), 
                              subtype = type,
                              spec = spec, 
                              owner = owner,
                              split_algo = split)
        
        self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return True #or raise