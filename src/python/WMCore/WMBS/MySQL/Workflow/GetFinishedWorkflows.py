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
    It assumes that a spec is not shared by two workflows of the same name.
    """

    incompleteWf = """ SELECT wmbs_workflow.name
                                  FROM wmbs_workflow
                                      INNER JOIN wmbs_subscription ON
                                         wmbs_subscription.workflow = wmbs_workflow.id
                                      INNER JOIN wmbs_sub_types ON
                                         wmbs_sub_types.id = wmbs_subscription.subtype
                                  WHERE wmbs_subscription.finished = 0
                                       AND wmbs_sub_types.name %(include)s IN ('LogCollect', 'Cleanup')
                                  GROUP BY wmbs_workflow.name """

    sql = """ SELECT wmbs_workflow.name, wmbs_workflow.spec,
                        wmbs_workflow.id AS workflow_id, wmbs_subscription.id AS sub_id
                 FROM wmbs_subscription
                    INNER JOIN wmbs_workflow ON
                         wmbs_workflow.id = wmbs_subscription.workflow
                    INNER JOIN wmbs_sub_types ON
                         wmbs_sub_types.id = wmbs_subscription.subtype
                    WHERE wmbs_sub_types.name %(include)s IN ('LogCollect', 'Cleanup')  AND
                            wmbs_workflow.name NOT IN (""" + incompleteWf + """ )"""

    def execute(self, onlySecondary=False, conn=None, transaction=False):
        """
        _execute_

        onlySecondary if set it True gets the complete subscription for the only LogCollect and Cleanup type.
        if False, gets the finished subscription excluding LogCollect and Cleanup tasks
        This DAO is a nested dictionary with the following structure:
        {<workflowName? : {spec : <specURL>,
                           workflows : {<workflowID> : [<subId1>, <subId2>]}
                          }
        }
        """
        if not onlySecondary:
            include = {'include': 'NOT' }
        else:
            include = {'include': '' }

        sql = self.sql % include

        #Get the completed workflows and subscriptions
        result = self.dbi.processData(sql, conn=conn, transaction=transaction)
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
