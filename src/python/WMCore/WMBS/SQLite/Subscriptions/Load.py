#!/usr/bin/env python
"""
_Load_

SQLite implementation of Subscription.Load
            
TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
"""
__all__ = []
__revision__ = "$Id: Load.py,v 1.2 2008/07/21 14:27:05 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.Load import Load as LoadMySQL

class Load(LoadMySQL, SQLiteBase):
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