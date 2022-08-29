"""
_NewSubscription_

Oracle implementation of DBS3Buffer.NewSubscription

Created on May 2, 2013

@author: dballest
"""

from WMComponent.DBS3Buffer.MySQL.NewSubscription import NewSubscription as MySQLNewSubscription

class NewSubscription(MySQLNewSubscription):
    """
    _NewSubscription_

    Create a new subscription in the database
    """

    sql = """INSERT INTO dbsbuffer_dataset_subscription
             (id, dataset_id, site, custodial, priority, subscribed, delete_blocks, dataset_lifetime)
             SELECT dbsbuffer_dataset_sub_seq.nextval, :id, :site, :custodial,
                    :priority, 0, :delete_blocks, :dataset_lifetime
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_dataset_subscription
                 WHERE dataset_id = :id
                 AND site = :site
                 AND custodial = :custodial
                 AND priority = :priority
                 AND dataset_lifetime = :dataset_lifetime )
             """
