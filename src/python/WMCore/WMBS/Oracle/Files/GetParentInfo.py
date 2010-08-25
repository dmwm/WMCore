#!/usr/bin/env python
"""
_GetParentInfo_

Oracle implementation of Files.GetParentInfo
"""

__revision__ = "$Id: GetParentInfo.py,v 1.1 2009/12/21 20:46:53 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Files.GetParentInfo import GetParentInfo as GetParentInfoMySQL

class GetParentInfo(GetParentInfoMySQL):
    sql = """SELECT wmbs_file_details.id,
                    wmbs_file_details.lfn,
                    wmbs_file_details.merged,
                    wmbs_file_gpdetails.lfn AS gplfn,
                    wmbs_file_gpdetails.merged AS gpmerged,
                    redneck_parent_output.output_fileset AS redneck_parent_fileset,
                    redneck_child_output.output_fileset AS redneck_child_fileset
                    FROM wmbs_file_details
               INNER JOIN wmbs_file_parent ON
                 wmbs_file_details.id = wmbs_file_parent.parent
               LEFT OUTER JOIN wmbs_file_parent wmbs_file_gparent ON
                 wmbs_file_parent.parent = wmbs_file_gparent.child
               LEFT OUTER JOIN wmbs_file_details wmbs_file_gpdetails ON
                 wmbs_file_gparent.parent = wmbs_file_gpdetails.id
               INNER JOIN wmbs_fileset_files ON
                 wmbs_file_parent.child = wmbs_fileset_files.fileid
               INNER JOIN wmbs_workflow_output ON
                 wmbs_fileset_files.fileset = wmbs_workflow_output.output_fileset
               INNER JOIN wmbs_subscription ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               INNER JOIN wmbs_workflow_output wmbs_pworkflow_output ON
                 wmbs_subscription.fileset = wmbs_pworkflow_output.output_fileset
               LEFT OUTER JOIN wmbs_workflow_output redneck_parent_output ON
                 wmbs_pworkflow_output.workflow_id = redneck_parent_output.workflow_id AND
                 wmbs_pworkflow_output.output_parent = redneck_parent_output.output_identifier
               LEFT OUTER JOIN wmbs_workflow_output redneck_child_output ON
                 wmbs_pworkflow_output.workflow_id = redneck_child_output.workflow_id AND
                 wmbs_pworkflow_output.output_identifier = redneck_child_output.output_parent
               WHERE wmbs_file_parent.child =
                 (SELECT id FROM wmbs_file_details WHERE lfn = :child_lfn)"""
