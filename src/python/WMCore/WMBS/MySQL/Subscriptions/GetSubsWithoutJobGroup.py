#!/usr/bin/env python

from __future__ import division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class GetSubsWithoutJobGroup(DBFormatter):
    """
    _GetSubsWithoutJobGroup_

    Finds whether there are unfinished subscriptions for Production and
    Processing task types where JobCreator hasn't yet created any jobs
    nor a jobgroup associated to it.
    """

    sql = """SELECT wmbs_subscription.id, wmbs_workflow.task FROM wmbs_subscription
               INNER JOIN wmbs_sub_types ON wmbs_sub_types.id = wmbs_subscription.subtype
               INNER JOIN wmbs_workflow ON wmbs_workflow.id = wmbs_subscription.workflow
               WHERE wmbs_subscription.finished=0 AND
                 wmbs_sub_types.name IN ('Production','Processing') AND
                 NOT EXISTS (SELECT * FROM wmbs_jobgroup
                   WHERE wmbs_jobgroup.subscription = wmbs_subscription.id)
          """

    def format(self, result):
        """
        Have to filter task names that contain only two slashes '/',
        such that we can declare those tasks as top level task.
        :param result: 
        :return: a list of subscriptions id
        """
        results = DBFormatter.format(self, result)

        subIDs = []
        for row in results:
            if len(row[1].split('/')) <= 3:  # remember, first item is empty
                subIDs.append(row[0])

        return subIDs

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn, transaction=transaction)
        return self.format(result)
