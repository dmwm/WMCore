#!/usr/bin/env python
"""
_ListWorkflowEfficiency_

Oracle implementation of Monitoring.ListWorkflowEfficiency
"""

__revision__ = "$Id: ListWorkflowEfficiency.py,v 1.1 2010/01/26 21:37:19 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Monitoring.ListWorkflowEfficiency import ListWorkflowEfficiency \
    as ListWorkflowEfficiencyMySQL

class ListWorkflowEfficiency(ListWorkflowEfficiencyMySQL):
    sql = """SELECT wmbs_workflow_output.output_identifier AS output_module,
                    output_fileset_details.total_files AS output_files,
                    output_fileset_details.total_events AS output_events,
                    input_fileset_details.total_files AS input_files,
                    input_fileset_details.total_events AS input_events FROM wmbs_subscription
               LEFT OUTER JOIN wmbs_workflow_output ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               LEFT OUTER JOIN
                 (SELECT wmbs_fileset_files.fileset AS fileset,
                         COUNT(wmbs_fileset_files.fileid) AS total_files,
                         SUM(wmbs_file_details.events) AS total_events FROM wmbs_fileset_files
                    INNER JOIN wmbs_file_details ON
                      wmbs_fileset_files.fileid = wmbs_file_details.id
                  GROUP BY wmbs_fileset_files.fileset) output_fileset_details ON
                 wmbs_workflow_output.output_fileset = output_fileset_details.fileset
               INNER JOIN
                 (SELECT wmbs_fileset_files.fileset AS fileset,
                         COUNT(wmbs_fileset_files.fileid) AS total_files,
                         SUM(wmbs_file_details.events) AS total_events FROM wmbs_fileset_files
                    INNER JOIN wmbs_file_details ON
                      wmbs_fileset_files.fileid = wmbs_file_details.id
                  GROUP BY wmbs_fileset_files.fileset) input_fileset_details ON
                 wmbs_subscription.fileset = input_fileset_details.fileset 
             WHERE wmbs_subscription.id = :subscriptionId"""
