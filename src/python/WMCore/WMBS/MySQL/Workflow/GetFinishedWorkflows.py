#!/usr/bin/env python
"""
_GetFinishedWorkflows_

MySQL implementation of Workflows.GetFinishedWorkflows

Created on Aug 29, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedWorkflows(DBFormatter):
    """
    A workflow is finished when all subscriptions associated to it are finished
    and all the other workflows using the same spec are finished as well,
    this DAO searches such workflows. It assumes that a spec is not shared by
    two workflows of the same name.
    """

    sql = """SELECT wmbs_workflow.name, wmbs_workflow.spec,
                        wmbs_workflow.id AS workflow_id, wmbs_subscription.id AS sub_id
                 FROM wmbs_subscription
                     INNER JOIN wmbs_workflow ON
                         wmbs_workflow.id = wmbs_subscription.workflow
                     INNER JOIN ( SELECT wmbs_workflow.name
                                  FROM wmbs_workflow
                                      LEFT OUTER JOIN wmbs_subscription ON
                                         wmbs_subscription.workflow = wmbs_workflow.id AND
                                         wmbs_subscription.finished = 0
                                  GROUP BY wmbs_workflow.name
                                  HAVING COUNT(wmbs_subscription.workflow) = 0 ) complete_workflow ON
                         complete_workflow.name = wmbs_workflow.name
              """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        This DAO is a nested dictionary with the following structure:
        {<workflowName? : {spec : <specURL>,
                           workflows : {<workflowID> : [<subId1>, <subId2>]}
                          }
        }
        """

        #Get the completed workflows and subscriptions
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = transaction)
        wfsAndSubs = self.formatDict(result)

        wfs = {}
        for entry in wfsAndSubs:
            if entry['name'] not in wfs:
                wfs[entry['name']] = {'workflows' : {}}
            wfs[entry['name']]['spec'] = entry['spec']
            if entry['workflow_id'] not in wfs[entry['name']]['workflows']:
                wfs[entry['name']]['workflows'][entry['workflow_id']] = []
            wfs[entry['name']]['workflows'][entry['workflow_id']].append(entry['sub_id'])

        return wfs
