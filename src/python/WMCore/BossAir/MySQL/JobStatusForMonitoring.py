#!/usr/bin/env python
"""
_JobStatusMonitoring_

MySQL implementation for loading a job by scheduler status
"""

from future.utils import viewitems

from WMCore.Database.DBFormatter import DBFormatter

class JobStatusForMonitoring(DBFormatter):
    """
    _LoadForMonitoring_

    Load all jobs with a certain scheduler status including
    all the joined information.
    """


    sql = """SELECT STRAIGHT_JOIN wwf.name as workflow, count(rj.wmbs_id) AS num_jobs,
                    st.name AS status, wl.plugin AS plugin, wu.cert_dn AS owner
               FROM bl_runjob rj
               LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN bl_status st ON rj.sched_status = st.id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               INNER JOIN wmbs_jobgroup wjg ON wjg.id = wj.jobgroup
               INNER JOIN wmbs_subscription ws ON ws.id = wjg.subscription
               INNER JOIN wmbs_workflow wwf ON wwf.id = ws.workflow
               LEFT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.status = :complete
               GROUP BY wwf.name, plugin, st.name
    """

    def mappedStatusFormat(self, results):
        """
        convert each indiviual batch system plugin status to a common status.
        Warning: This assumes all the plugin are under WMCore/BossAir/Plugins/
        and its module name and class name should be the same.

        __import__ doesn't reload if the module exist.
        (so doesn't need to keep track what is already imported.
        The performance difference should be small. If desired, maintain
        the cache to keep track imported plugIns
        """
        commonStates = {}
        for data in results:
            module = __import__("WMCore.BossAir.Plugins.%s" % data['plugin'],
                                globals(), locals(), [data['plugin']])
            plugIn = getattr(module, data['plugin'])
            state = plugIn.stateMap().get(data['status'])
            if data['workflow'] not in commonStates:
                commonStates[data['workflow']] = {}

            commonStates[data['workflow']].setdefault(state, 0)
            commonStates[data['workflow']][state] += data['num_jobs']

        results = []
        for key, value in viewitems(commonStates):
            reformedData = {'request_name': key}
            reformedData.update(value)
            results.append(reformedData)
        return results


    def execute(self, commonFormat = True, conn = None, transaction = False):
        """
        _execute_

        Load all jobs either running or not (running by default)
        """
        complete = '1'
        binds = {'complete': complete}


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        if commonFormat:
            return self.mappedStatusFormat(self.formatDict(result))
        else:
            return self.formatDict(result)
