#!/usr/bin/env python
"""
_StatusDB_

"""




import threading
import time

#from WMCore.BossLite.Common.Exceptions  import DbError
#from WMCore.BossLite.Common.System      import evalCustomList
#from WMCore.BossLite.API.BossLiteDBInterface    import BossLiteDBInterface

from WMCore.BossLite.DbObjects.BossLiteDBWM  import BossLiteDBWM

#class StatusDB(BossLiteDBInterface):
class StatusDB(BossLiteDBWM):
    """
    Class specialized on using WMCore.BossLite database for status tracking
    """

    def __init__(self):
        """
        __init__
        """

        # call super class init method
        super(StatusDB, self).__init__()

        # Initialize WMCore database ...
        #self.engine = WMConnectionBase(daoPackage = "WMCore.BossLite")

        #self.existingTransaction = None


    def __executeQuery(self, query):
        """
        __executeQuery__
        uses the general-purpose DAO to make query
        TODO: convert most of queries into DAO objects
        """
        result = None
        if len(query) > 2:
            queryResult = self.executeSQL(query)
            result = queryResult[0].fetchall()
        return result

    def getJobsStatistic(self):
        """
        __setJobs__

        set job status for a set of jobs
        """

        # build query
        query = """
        select status, count( status ) from bl_runningjob
        where closed='N' group by  status
        """

        return self.__executeQuery(query)


    def getUnprocessedJobs(self, grlist):
        """
        __getUnprocessedJobs__

        select jobs not yet assigned to a status query group
        """
        queryAddin = "where group_id is not null "
        if grlist is not None and len(grlist) > 0:
            queryAddin = "where group_id not in (%s) " % str(grlist)

        # some groups with threads working on
        query = " select task_id, count(job_id) from jt_group %s" % queryAddin
        query += " group by task_id order by count(job_id) desc"

        return self.__executeQuery(query)

    def getUnassociatedJobs(self):
        """
        __getUnassociatedJobs__

        select active jobs not yet associated to a status query group
        """

        query = 'select j.task_id,j.job_id from bl_runningjob j' \
              + ' left join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where g.job_id IS NULL ' \
              + " and j.closed='N' and j.scheduler_id IS NOT NULL" \
              + " and j.process_status like '%%handled'" \
              + " order by j.task_id"
        return self.__executeQuery(query)

    def addForCheck(self, taskId, jobId ):
        """
        __addForCheck__

        insert job in the query queue
        """

        query = \
              "insert into jt_group(group_id, task_id, job_id)" + \
              " values(0," + str( taskId ) + ',' + str( jobId ) + \
              ') on duplicate key update group_id=group_id'

        self.__executeQuery(query)

    def addForCheckMultiple(self, pairs):
        """
        __addForCheck__

        insert job in the query queue
        """

        values = ''
        first = True
        for t, j in pairs:
            values += "(0,%s,%s),"%(str(t), str(j))
        ## remove extra comma
        values = values[:-1]
           
        query = \
              "insert into jt_group(group_id, task_id, job_id)" + \
              " values" + values + " on duplicate key update group_id=group_id"

        self.__executeQuery(query)


    def setTaskGroup( self, group, taskList ) :
        """
        __setTaskGroup__

        assign tasks to a given group
        """

        query = \
              'update jt_group set group_id=' + str(group) + \
              ' where task_id in (' + taskList  + ')'

        self.__executeQuery(query)

    def getAssociatedJobs(self):
        """
        __getAssociatedJobs__

        select active jobs associated to a status query group

        """

        query = \
              'select g.task_id,g.job_id from jt_group g left join ' \
              + ' (select task_id,job_id from bl_runningjob ' \
              + " where closed='N' and scheduler_id IS NOT NULL " \
              + " and process_status like '%%handled') j " \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where j.job_id IS NULL'

        return self.__executeQuery(query)

    def removeFromCheck(self, taskId, jobId ):#group, taskId, jobId ):
        """
        __removeFromCheck__

        remove job from the query queue
        """

        query = \
                'delete from jt_group where task_id=' + str( taskId ) \
                        + ' and job_id=' + str( jobId )

        self.__executeQuery(query)

    def processBulkUpdate( self, jobList, processStatus, skipStatus=None ) :
        """
        __processBulkUpdate__

        bulk update of job process_status
        """

        jlist = ','.join( [ str(job.runningJob['id']) for job in jobList ] )

        if skipStatus is not None:
            toSkip = " and status not in ('" +  "','".join( skipStatus ) + "')"
        else :
            toSkip = ''

        if processStatus in ['failed', 'output_requested'] :
            tsString = "', output_request_time='" + \
                       time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime() )
        else :
            tsString = ''


        query = \
              "update bl_runningjob set process_status='" + processStatus + \
              tsString + "' where id in (" + jlist + ")" + toSkip

        self.__executeQuery(query)


    def getGroupTasks(self, group):
        """
        __getGroupTasks__

        retrieves tasks for a given group
        """

        query = "select distinct(task_id) from jt_group where group_id=" \
                + str(group)

        rows = self.__executeQuery(query)

        return [int(key[0]) for key in rows ]

