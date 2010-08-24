#!/usr/bin/env python
"""
_Exists_

SQLite implementation of Subscription.Exists

TABLE wmbs_subscription
    id      INT(11) NOT NULL AUTO_INCREMENT,
    fileset INT(11) NOT NULL,
    workflow INT(11) NOT NULL,
    type    ENUM("merge", "processing")
    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/07/21 14:27:06 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.SQLite.Base import SQLiteBase
from WMCore.WMBS.MySQL.Subscriptions.Exists import Exists as ExistsMySQL

class Exists(ExistsMySQL, SQLiteBase):
    sql = ExistsMySQL.sql