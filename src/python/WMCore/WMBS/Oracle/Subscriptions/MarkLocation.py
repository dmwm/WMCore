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
__revision__ = "$Id: MarkLocation.py,v 1.3 2009/05/18 16:52:08 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.MarkLocation import MarkLocation as MarkLocationMySQL

class MarkLocation(MarkLocationMySQL):
    sql = """insert into wmbs_subscription_location 
             (subscription, location, valid)
             select :subscription, id, :valid from wmbs_location 
             where site_name = :location"""
