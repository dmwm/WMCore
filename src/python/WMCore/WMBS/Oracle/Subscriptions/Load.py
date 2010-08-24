#!/usr/bin/env python
"""
_Load_

Oracle implementation of Subscription.Load
            
TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.WMBS.MySQL.Subscriptions.Load import Load as LoadMySQL

class Load(LoadMySQL):
    
    def getSQL(self, **kwargs):
        
        sql = ""
        binds = {}
        if kwargs['id'] != -1:
            sql = """select id, fileset, workflow, subtype, split_algo from wmbs_subscription 
            where id = :id"""
            binds = self.getBinds(id=kwargs['id'])
        else:
            sql = """select id, fileset, workflow, subtype, split_algo from wmbs_subscription 
            where fileset = :fileset and workflow = :workflow and 
                 subtype = (select id from wmbs_subs_type where name = :type)"""
            binds = self.getBinds(fileset=int(kwargs['fileset']), 
                                  workflow=int(kwargs['workflow']),
                                  type=kwargs['type'])
        return sql, binds
    