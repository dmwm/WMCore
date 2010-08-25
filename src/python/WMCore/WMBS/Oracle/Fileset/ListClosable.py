#!/usr/bin/env python
"""
_ListClosable_

MySQL implementation of Fileset.ListClosable
"""

__all__ = []
__revision__ = "$Id: ListClosable.py,v 1.2 2009/09/03 19:47:26 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.ListClosable import ListClosable as ListFilesetClosableMySQL

class ListClosable(ListFilesetClosableMySQL):
    sql = """SELECT wmbs_workflow_output.output_fileset FROM wmbs_subscription
               INNER JOIN wmbs_fileset ON
                 wmbs_subscription.fileset = wmbs_fileset.id
               INNER JOIN wmbs_workflow_output ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               INNER JOIN (SELECT id, open FROM wmbs_fileset) output_fileset ON
                 wmbs_workflow_output.output_fileset = output_fileset.id                 
               INNER JOIN (SELECT fileset, COUNT(fileid) AS total_files
                            FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                 wmbs_subscription.fileset = fileset_size.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(fileid) AS total_files
                   FROM wmbs_sub_files_complete GROUP BY subscription) sub_complete ON
                 wmbs_subscription.id = sub_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(fileid) AS total_files
                   FROM wmbs_sub_files_failed GROUP BY subscription) sub_failed ON
                 wmbs_subscription.id = sub_failed.subscription
             WHERE wmbs_fileset.open = '0' AND output_fileset.open = 1 AND 
               fileset_size.total_files = (COALESCE(sub_complete.total_files, 0) +
                                           COALESCE(sub_failed.total_files, 0))
    """                            
