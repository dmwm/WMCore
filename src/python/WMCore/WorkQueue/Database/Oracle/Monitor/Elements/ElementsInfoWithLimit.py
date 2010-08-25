""""
_ElementsInfoWithLimit_

Oracle implementation of Monitor.Elements.ElementsInfoWithLimit
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Elements.ElementsInfoWithLimit \
     import ElementsInfoWithLimit as ElementsInfoWithLimitMySQL

class ElementsInfoWithLimit(ElementsInfoWithLimitMySQL):
    """
    Use pagination (and synchronize with YUI table) 
    """
    #TODO need to fix the query to oracle specific use rownum
    sql = """SELECT we.id as id, ws.name as spec_name, wt.name as task_name, 
                    wd.name as element_name, parent_queue_id, wq.url as child_queue, num_jobs,
                    priority, parent_flag, status, subscription_id, insert_time, update_time
             FROM wq_element we
             INNER JOIN wq_wmtask wt ON (wt.id = we.wmtask_id) 
             INNER JOIN wq_wmspec ws ON (ws.id = wt.wmspec_id)
             LEFT OUTER JOIN wq_data wd ON (wd.id = we.input_id)
             LEFT OUTER JOIN wq_queues wq ON (wq.id = we.child_queue)
             ORDER BY we.id
             LIMIT :startIndex, :results """
    