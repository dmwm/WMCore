#!/usr/bin/env python
"""
_GetMergedChildren_

Find the merged children of the input to a processing job.  This is used by the
JobAccountant to track down redneck parents that were produced after their
children.

Given an input file that was used in a redneck parentage workflow and an output
fileset for a particular output module of the workflow, find any merged files
that were produced either directly from the processing job or through a merge
job.

This will use the output fileset from the job to find a merge subscription.
From there it will use the output map from the merge workflow to find the merged
output fileset.  That fileset will be searched for any files that have the
original input file as a parent or grand parent. 
"""




import logging

from WMCore.Database.DBFormatter import DBFormatter

class GetMergedChildren(DBFormatter):
    sql = """SELECT wmbs_file_details.lfn FROM wmbs_subscription
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
               INNER JOIN wmbs_workflow_output ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               INNER JOIN wmbs_fileset_files ON
                 wmbs_workflow_output.output_fileset = wmbs_fileset_files.fileset
               LEFT OUTER JOIN wmbs_file_parent ON
                 wmbs_fileset_files.file = wmbs_file_parent.child
               LEFT OUTER JOIN wmbs_file_parent wmbs_file_gparent ON
                 wmbs_file_parent.parent = wmbs_file_gparent.child
               LEFT OUTER JOIN wmbs_file_details ON
                 wmbs_fileset_files.file = wmbs_file_details.id
             WHERE wmbs_sub_types.name = 'Merge' AND
                   wmbs_subscription.fileset = :parent_fileset AND
                   (wmbs_file_parent.parent =
                      (SELECT id FROM wmbs_file_details WHERE lfn = :input_lfn) OR
                    wmbs_file_gparent.parent =
                       (SELECT id FROM wmbs_file_details WHERE lfn = :input_lfn))"""

    def format(self, results):
        """
        _format_

        Format the result into a set of LFNs.
        """
        results = DBFormatter.format(self, results)

        lfnSet = set()
        for result in results:
            lfnSet.add(result[0])

        return lfnSet

    def execute(self, inputLFN, parentFileset, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"input_lfn": inputLFN,
                                                 "parent_fileset": parentFileset}, 
                                      conn = conn, transaction = transaction)
        result = self.format(result)
        return result
