#!/usr/bin/env python
"""
_JobStatusByLocation_

MySQL implementation for loading a job by scheduler status
"""

from future.utils import viewitems, viewvalues

from WMCore.Database.DBFormatter import DBFormatter

class JobStatusByLocation(DBFormatter):
    """
    _JobStatusByLocation_

    Load all jobs with a certain scheduler status including
    all the joined information.
    """


    sql = """SELECT wl.cms_name AS site_name, wl.pending_slots as pending_slots,
                    count(rj.wmbs_id) AS num_jobs,
                    st.name AS status, wl.plugin AS plugin
               FROM bl_runjob rj
               INNER JOIN bl_status st ON rj.sched_status = st.id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               RIGHT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.status = :complete
               GROUP BY wl.cms_name, wl.plugin, st.name, wl.pending_slots
          """

    def mappedStatusFormat(self, results):
        """
        convert each individual batch system plugin status to a common status.
        Warning: This assumes all the plugin are under WMCore/BossAir/Plugins/
        and its module name and class name should be the same.

        __import__ doesn't reload if the module exist.
        (so doesn't need to keep track what is already imported.
        The performance difference should be small. If desired, maintain
        the cache to keep track imported plugIns
        """
        commonStates = {}
        for data in results:
            if data['site_name'] not in commonStates:
                commonStates[data['site_name']] = {}

            module = __import__("WMCore.BossAir.Plugins.%s" % data['plugin'],
                                globals(), locals(), [data['plugin']])
            plugIn = getattr(module, data['plugin'])

            for status in viewvalues(plugIn.stateMap()):
                commonStates[data['site_name']].setdefault(status, 0)

            state = plugIn.stateMap().get(data['status'])
            commonStates[data['site_name']][state] += data['num_jobs']
            commonStates[data['site_name']]['pending_slots'] = data['pending_slots']

        results = []
        for key, value in viewitems(commonStates):
            reformedData = {'site_name': key}
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
