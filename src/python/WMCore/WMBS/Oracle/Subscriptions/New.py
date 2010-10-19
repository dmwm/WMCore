#!/usr/bin/env python
"""
_Subscription.New_

Oracle implementation of Subscription.New
"""

from WMCore.WMBS.MySQL.Subscriptions.New import New as NewMySQL

class New(NewMySQL):
    sql = """INSERT INTO wmbs_subscription (id, fileset, workflow, subtype,
                                            split_algo, last_update) 
               SELECT wmbs_subscription_SEQ.nextval, :fileset, :workflow, id,
                      :split_algo, :timestamp FROM wmbs_sub_types
               WHERE name = :subtype"""

    sqlAvail = """INSERT INTO wmbs_sub_files_available (subscription, fileid)
                    SELECT wmbs_subscription.id, wmbs_fileset_files.fileid
                           FROM wmbs_fileset_files
                      INNER JOIN wmbs_subscription ON
                        wmbs_subscription.workflow = :workflow AND
                        wmbs_subscription.fileset = :fileset AND
                        wmbs_fileset_files.fileset = wmbs_subscription.fileset"""
