#!/usr/bin/env python
"""
_Subscription.New_

Oracle implementation of Subscription.New
"""

__revision__ = "$Id: New.py,v 1.3 2009/10/12 21:11:12 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    """
    Sequence.
    """
    sql = """INSERT INTO wmbs_subscription (id, fileset, workflow, subtype,
                                            split_algo, last_update) 
               SELECT wmbs_subscription_SEQ.nextval, :fileset, :workflow, id,
                      :split_algo, :timestamp FROM wmbs_sub_types
               WHERE name = :subtype""" 
