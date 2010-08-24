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
__revision__ = "$Id: MarkLocation.py,v 1.1 2008/11/11 09:34:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.SQLite.Subscriptions.MarkLocation import MarkLocation as JobsSQLite

class MarkLocation(JobsSQLite):
    sql = JobsSQLite.sql