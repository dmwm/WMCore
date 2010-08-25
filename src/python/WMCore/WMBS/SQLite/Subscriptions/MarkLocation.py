#!/usr/bin/env python
"""
_MarkLocation_

SQLite implementation of Subscription.MarkLocation

Insert a record to wmbs_subscription_location

CREATE TABLE wmbs_subscription_location (
             subscription     INT(11)      NOT NULL,
             location         INT(11)      NOT NULL,
             valid            BOOLEAN      NOT NULL DEFAULT TRUE,
             FOREIGN KEY(subscription)  REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY(location)     REFERENCES wmbs_location(id)
               ON DELETE CASCADE)
"""
__all__ = []
__revision__ = "$Id: MarkLocation.py,v 1.4 2009/05/09 11:42:27 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class MarkLocation(DBFormatter):
    sql = """insert into wmbs_subscription_location 
            (subscription, location, valid)
        values (
            :subscription, 
            (select id from wmbs_location where site_name = :location),
            :valid
        )"""
    
    def execute(self, subscription=None, location=None, valid = None, 
                conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, 
                                      self.getBinds(subscription=subscription, 
                                              location=location, valid=valid), 
                         conn = conn, transaction = transaction)
        return self.format(result)
