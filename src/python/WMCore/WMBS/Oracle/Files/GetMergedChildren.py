#!/usr/bin/env python
"""
_GetMergedChildren_

Oracle implementation of Files.GetMergedChildren
"""

__revision__ = "$Id: GetMergedChildren.py,v 1.1 2009/12/18 17:54:08 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetMergedChildren import GetMergedChildren as GetMergedChildrenMySQL

class GetMergedChildren(GetMergedChildrenMySQL):
    sql = """SELECT wmbs_file_details.lfn FROM wmbs_subscription
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
               INNER JOIN wmbs_workflow_output ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               INNER JOIN wmbs_fileset_files ON
                 wmbs_workflow_output.output_fileset = wmbs_fileset_files.fileset
               LEFT OUTER JOIN wmbs_file_parent ON
                 wmbs_fileset_files.fileid = wmbs_file_parent.child
               LEFT OUTER JOIN wmbs_file_parent wmbs_file_gparent ON
                 wmbs_file_parent.parent = wmbs_file_gparent.child
               LEFT OUTER JOIN wmbs_file_details ON
                 wmbs_fileset_files.fileid = wmbs_file_details.id
             WHERE wmbs_sub_types.name = 'Merge' AND
                   wmbs_subscription.fileset = :parent_fileset AND
                   (wmbs_file_parent.parent =
                      (SELECT id FROM wmbs_file_details WHERE lfn = :input_lfn) OR
                    wmbs_file_gparent.parent =
                       (SELECT id FROM wmbs_file_details WHERE lfn = :input_lfn))"""

