#!/usr/bin/env python
"""
_JobStatusByWorkflowAndSite_

MySQL implementation for loading a job by scheduler status
"""


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossAir.Plugins.BasePlugin import BasePlugin

class JobStatusByTaskAndSite(DBFormatter):
    """
    _LoadForMonitoring_

    Load all jobs with a certain scheduler status including
    all the joined information.
    """

    #TODO: not sure why location is left outer join instead of inner join
    sql = """SELECT wwf.name as workflow, wwf.task, count(rj.wmbs_id) AS num_jobs,
                    st.name AS status, wl.cms_name AS site,
                    wl.plugin AS plugin, wu.cert_dn AS owner
               FROM bl_runjob rj
               INNER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN bl_status st ON rj.sched_status = st.id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               INNER JOIN wmbs_jobgroup wjg ON wjg.id = wj.jobgroup
               INNER JOIN wmbs_subscription ws ON ws.id = wjg.subscription
               INNER JOIN wmbs_workflow wwf ON wwf.id = ws.workflow
               LEFT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.status = :complete
               GROUP BY wwf.name, wwf.task, plugin, st.name, wl.cms_name, wu.cert_dn
    """
    #TODO this needs to move to BasePlugin replacing the global state: check with Matt
    MONITOR_STATE_MAP = {'Pending': 'submitted_pending',
                         'Running': 'submitted_running',
                         'Complete': 'submitted_running',
                         'Error': 'submitted_running'}

    def mappedStatusFormat(self, results):
        """
        convert each indiviual batch system plugin status to a common status.
        Warning: This assumes all the plugin are under WMCore/BossAir/Plugins/
        and its module name and class name should be the same.

        __import__ doesn't reload if the module exist.
        (so doesn't need to keep track what is already imported.
        The performance difference should be small. If desired, maintain
        the cache to keep track imported plugIns
        TODO: need to reduce state only pending_batch and running_batch
        """
        commonStates = {}
        for data in results:
            module = __import__("WMCore.BossAir.Plugins.%s" % data['plugin'],
                                globals(), locals(), [data['plugin']])
            plugIn = getattr(module, data['plugin'])
            state = self.MONITOR_STATE_MAP[plugIn.stateMap().get(data['status'])]
            commonStates.setdefault(data['workflow'], {})
            commonStates[data['workflow']].setdefault('tasks', {})
            commonStates[data['workflow']]['tasks'].setdefault(data['task'], {})
            commonStates[data['workflow']]['tasks'][data['task']].setdefault(state, {})
            commonStates[data['workflow']]['tasks'][data['task']][state].setdefault(data['site'], 0)
            commonStates[data['workflow']]['tasks'][data['task']][state][data['site']] += data['num_jobs']
        return commonStates

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        Load all jobs either running or not (running by default)
        """
        complete = '1'
        binds = {'complete': complete}


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.mappedStatusFormat(self.formatDict(result))
