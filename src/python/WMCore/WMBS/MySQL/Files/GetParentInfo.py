#!/usr/bin/env python
"""
_GetParentInfo_

Figure out parentage information for a file in WMBS.  This will return
information about a file's parent and it's grand parent such as the
lfn, id and whether or not the file is merged.  This will also determine
whether or not the file is a redneck parent or redneck child.
"""

__revision__ = "$Id: GetParentInfo.py,v 1.2 2009/12/23 17:49:36 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetParentInfo(DBFormatter):
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
                 wmbs_file_parent.child = wmbs_fileset_files.file
               INNER JOIN wmbs_workflow_output ON
                 wmbs_fileset_files.fileset = wmbs_workflow_output.output_fileset
               INNER JOIN wmbs_subscription ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               LEFT OUTER JOIN wmbs_workflow_output wmbs_pworkflow_output ON
                 wmbs_subscription.fileset = wmbs_pworkflow_output.output_fileset
               LEFT OUTER JOIN wmbs_workflow_output redneck_parent_output ON
                 wmbs_pworkflow_output.workflow_id = redneck_parent_output.workflow_id AND
                 wmbs_pworkflow_output.output_parent = redneck_parent_output.output_identifier
               LEFT OUTER JOIN wmbs_workflow_output redneck_child_output ON
                 wmbs_pworkflow_output.workflow_id = redneck_child_output.workflow_id AND
                 wmbs_pworkflow_output.output_identifier = redneck_child_output.output_parent
               WHERE wmbs_file_parent.child =
                 (SELECT id FROM wmbs_file_details WHERE lfn = :child_lfn)"""
    
    def execute(self, childLFNs, conn = None, transaction = False):
        bindVars = []
        for childLFN in childLFNs:
            bindVars.append({"child_lfn": childLFN})
            
        result = self.dbi.processData(self.sql, bindVars, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
