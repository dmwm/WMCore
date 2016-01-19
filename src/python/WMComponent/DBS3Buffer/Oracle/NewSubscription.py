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
             (id, dataset_id, site, custodial, auto_approve, move, priority, subscribed, phedex_group, delete_blocks)
             SELECT dbsbuffer_dataset_sub_seq.nextval, :id, :site, :custodial, :auto_approve,
                    :move, :priority, 0, :phedex_group, :delete_blocks
             FROM DUAL
             WHERE NOT EXISTS
               ( SELECT *
                 FROM dbsbuffer_dataset_subscription
                 WHERE dataset_id = :id
                 AND site = :site
                 AND custodial = :custodial
                 AND auto_approve = :auto_approve
                 AND move = :move
                 AND priority = :priority )
             """
