#!/usr/bin/env python
"""
_ListWorkflowEfficiency_

Determine the efficiency in terms of input events / output events for a
workflow.
"""

__revision__ = "$Id: ListWorkflowEfficiency.py,v 1.1 2010/01/26 21:37:18 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListWorkflowEfficiency(DBFormatter):
    sql = """SELECT wmbs_workflow_output.output_identifier AS output_module,
                    output_fileset_details.total_files AS output_files,
                    output_fileset_details.total_events AS output_events,
                    input_fileset_details.total_files AS input_files,
                    input_fileset_details.total_events AS input_events FROM wmbs_subscription
               LEFT OUTER JOIN wmbs_workflow_output ON
                 wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
               LEFT OUTER JOIN
                 (SELECT wmbs_fileset_files.fileset AS fileset,
                         COUNT(wmbs_fileset_files.file) AS total_files,
                         SUM(wmbs_file_details.events) AS total_events FROM wmbs_fileset_files
                    INNER JOIN wmbs_file_details ON
                      wmbs_fileset_files.file = wmbs_file_details.id
                  GROUP BY wmbs_fileset_files.fileset) output_fileset_details ON
                 wmbs_workflow_output.output_fileset = output_fileset_details.fileset
               INNER JOIN
                 (SELECT wmbs_fileset_files.fileset AS fileset,
                         COUNT(wmbs_fileset_files.file) AS total_files,
                         SUM(wmbs_file_details.events) AS total_events FROM wmbs_fileset_files
                    INNER JOIN wmbs_file_details ON
                      wmbs_fileset_files.file = wmbs_file_details.id
                  GROUP BY wmbs_fileset_files.fileset) input_fileset_details ON
                 wmbs_subscription.fileset = input_fileset_details.fileset 
             WHERE wmbs_subscription.id = :subscriptionId"""

    def format(self, results):
        """
        _format_

        Clean up the results to make it easier to render this as HTML.
        """
        results = self.formatDict(results)

        formattedResults = []
        for result in results:
            if result["output_files"] == None:
                result["output_files"] = 0
            if result["output_events"] == None:
                result["output_events"] = 0

            # For some reason these fields are being turned into "Decimal"
            # types.  I really want an int though...
            result["input_events"] = int(result["input_events"])
            result["output_events"] = int(result["output_events"])

            if result["input_events"] == 0:
                result["efficiency"] = "0.0%"
            else:
                result["efficiency"] = "%.2f%%" % ((float(result["output_events"]) / float(result["input_events"])) * 100)
                
            formattedResults.append(result)

        return formattedResults
    
    def execute(self, subscriptionId, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """        
        result = self.dbi.processData(self.sql, {"subscriptionId": subscriptionId},
                                      conn = conn, transaction = transaction)
        
        return self.format(result)
