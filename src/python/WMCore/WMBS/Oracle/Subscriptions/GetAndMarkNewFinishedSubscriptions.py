#!/usr/bin/env python
"""
_GetAndMarkNewFinishedSubscriptions_

Oracle implementation of Subscription.GetAndMarkNewFinishedSubscriptions
"""

from WMCore.WMBS.MySQL.Subscriptions.GetAndMarkNewFinishedSubscriptions \
        import GetAndMarkNewFinishedSubscriptions as MySQLGetAndMarkNewFinishedSubscriptions

class GetAndMarkNewFinishedSubscriptions(MySQLGetAndMarkNewFinishedSubscriptions):
    
    completeNonJobSQLSubQuery = """
                   WITH
                    complete_subscription
                   AS
                    (SELECT distinct wmbs_subscription.id,
                                    wmbs_subscription.fileset,
                                    wmbs_workflow.name
                        FROM wmbs_subscription
                        INNER JOIN wmbs_fileset ON
                            wmbs_fileset.id = wmbs_subscription.fileset AND
                            wmbs_fileset.open = 0
                        INNER JOIN wmbs_workflow ON
                            wmbs_workflow.id = wmbs_subscription.workflow AND
                            wmbs_workflow.injected = 1
                        LEFT OUTER JOIN wmbs_sub_files_available ON
                            wmbs_sub_files_available.subscription = wmbs_subscription.id
                        LEFT OUTER JOIN wmbs_sub_files_acquired ON
                            wmbs_sub_files_acquired.subscription = wmbs_subscription.id
                      WHERE wmbs_subscription.finished = 0 AND
                            wmbs_sub_files_available.subscription is Null AND
                            wmbs_sub_files_acquired.subscription is Null
                     )
                    """
    
    subWithUnfinishedJobSQL = """
                    SELECT complete_subscription.id 
                          FROM complete_subscription
                          INNER JOIN wmbs_jobgroup ON
                            wmbs_jobgroup.subscription = complete_subscription.id
                          INNER JOIN wmbs_job ON
                            wmbs_job.jobgroup = wmbs_jobgroup.id AND
                            wmbs_job.state_time > :maxTime AND
                            wmbs_job.state != %d
                     """
    sql = """ UPDATE wmbs_subscription
              SET wmbs_subscription.finished = 1, wmbs_subscription.last_update = :timestamp
              WHERE wmbs_subscription.id IN ( 
                    %s
                    
                    SELECT complete_subscription.id
                        FROM complete_subscription 

                        INNER JOIN wmbs_fileset ON
                            wmbs_fileset.id = complete_subscription.fileset
                        LEFT OUTER JOIN wmbs_fileset_files ON
                            wmbs_fileset_files.fileset = wmbs_fileset.id
                        LEFT OUTER JOIN wmbs_file_parent ON
                            wmbs_file_parent.parent = wmbs_fileset_files.fileid
                        LEFT OUTER JOIN wmbs_fileset_files child_fileset ON
                            child_fileset.fileid = wmbs_file_parent.child
                        LEFT OUTER JOIN wmbs_subscription child_subscription ON
                            child_subscription.fileset = child_fileset.fileset AND
                            child_subscription.finished = 0
                        LEFT OUTER JOIN wmbs_workflow child_workflow ON
                            child_subscription.workflow = child_workflow.id AND
                            child_workflow.name != complete_subscription.name

                    WHERE complete_subscription.id
                        NOT IN (
                          %s
                        )
                    GROUP BY complete_subscription.id
                    HAVING COUNT(child_workflow.name) = 0) """ % (completeNonJobSQLSubQuery, subWithUnfinishedJobSQL)
