"""
_CheckComponentStatus_

MySQL implementation of CheckComponentStatus
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class CheckComponentStatus(DBFormatter):

    sql = """SELECT comp.name as name
             FROM wm_workers worker
             INNER JOIN wm_components comp ON comp.id = worker.component_id
             INNER JOIN (SELECT component_id, MAX(last_updated) AS last_updated FROM wm_workers
                         GROUP BY component_id) max_result
                        ON (worker.last_updated = max_result.last_updated
                           AND max_result.component_id = comp.id)
             WHERE max_result.last_updated + comp.update_threshold < :current_time
             """
    #sql = """select max(last_updated) from wm_workers"""

    def execute(self,detail = False, conn = None, transaction = False):
        binds = {'current_time' : int(time.time())}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                  transaction = transaction)
        # If detail flag is true return list of components which were down
        resultList = self.format(result)
        if detail:
            componentList = []
            for a in resultList:
                componentList.append(a[0])
            return componentList

        if len(resultList) > 0:
            return {'status': 'down'}
        else:
            return {'status': 'ok'}
