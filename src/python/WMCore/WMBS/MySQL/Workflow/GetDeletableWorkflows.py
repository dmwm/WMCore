#!/usr/bin/env python
"""
_GetDeletableWorkflows_

MySQL implementation of Workflows.GetDeletableWorkflows

"""

from WMCore.Database.DBFormatter import DBFormatter

class GetDeletableWorkflows(DBFormatter):
    """
    A workflow is finished when all subscriptions associated to it are finished
    and all the other workflows using the same spec are finished as well,
    this DAO searches such workflows. It assumes that a spec is not shared by
    two workflows of the same name.
    """

    # Gets completed workflows.
    # (checks there is no unfinished subscription is left on given workflow)
    # Assumes there is no workflow created without subscription is made. (- need to check)
    # Query: all the workflows - workflows with non finished subscription

    compltetedWFs = """ SELECT name FROM wmbs_workflow
                            WHERE name NOT IN (
                                SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                      INNER JOIN wmbs_subscription ws
                                         ON ws.workflow = ww.id
                                WHERE ws.finished =0)
                      """

    # workflows which has not completed child workflow
    # Child workflow means that the workflow uses output files from the other workflow.
    wfsWithIncompletedChildWFs = """ SELECT DISTINCT ww.name FROM wmbs_workflow ww
                                  INNER JOIN wmbs_subscription ws
                                      ON ws.workflow = ww.id
                                  INNER JOIN wmbs_fileset wfs ON
                                     wfs.id = ws.fileset
                                  INNER JOIN wmbs_fileset_files wfsf ON
                                     wfsf.fileset = wfs.id
                                  INNER JOIN wmbs_file_parent wfp ON
                                     wfp.parent = wfsf.fileid
                                  INNER JOIN wmbs_fileset_files child_fileset ON
                                     child_fileset.fileid = wfp.child
                                  INNER JOIN wmbs_subscription child_subscription ON
                                     child_subscription.fileset = child_fileset.fileset
                                  WHERE child_subscription.finished = 0
                            """

    sql = """SELECT DISTINCT wmbs_workflow.name, wmbs_workflow.spec,
                        wmbs_workflow.id AS workflow_id, wmbs_subscription.id AS sub_id
                 FROM wmbs_subscription
                     INNER JOIN wmbs_workflow ON
                         wmbs_workflow.id = wmbs_subscription.workflow
                     INNER JOIN (%s) complete_workflow ON
                         complete_workflow.name = wmbs_workflow.name
                 WHERE wmbs_workflow.name NOT IN (%s)
              """ % (compltetedWFs, wfsWithIncompletedChildWFs)

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

