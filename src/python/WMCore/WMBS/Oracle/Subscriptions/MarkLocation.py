#!/usr/bin/env python
"""
_MarkLocation_

Oracle implementation of Subscription.MarkLocation

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
__revision__ = "$Id: MarkLocation.py,v 1.2 2008/11/24 21:51:46 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.MarkLocation import MarkLocation as MarkLocationMySQL

class MarkLocation(MarkLocationMySQL):
    sql = """insert into wmbs_subscription_location 
             (subscription, location, valid)
             select :subscription, id, :valid from wmbs_location 
             where se_name = :location"""