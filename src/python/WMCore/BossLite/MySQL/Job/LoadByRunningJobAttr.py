#!/usr/bin/env python
"""
_LoadByRunningJobAttr_

MySQL implementation of BossLite.Jobs.LoadByRunningJobAttr
"""

__all__ = []
__revision__ = "$Id: LoadByRunningJobAttr.py,v 1.1 2010/04/28 21:14:40 mnorman Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadByRunningJobAttr(DBFormatter):
    sql = """SELECT bl_job.job_id as jobId, bl_job.task_id as taskId,
                bl_job.name as name, bl_job.executable as executable, bl_job.events as events,
                bl_job.arguments as arguments, bl_job.stdin as StandardInput,
                bl_job.stdout as StandardOutput, bl_job.stderr as StandardError,
                bl_job.input_files as inputFiles, bl_job.output_files as outputFiles,
                bl_job.dls_destination as dlsDestination,
                bl_job.submission_number as submissionNumber,
                bl_job.closed as closed
                FROM bl_job
                INNER JOIN bl_runningjob ON bl_runningjob.job_id = bl_job.job_id
                WHERE bl_runningjob.submission = (SELECT MAX(submission) FROM bl_runningjob
                     WHERE bl_runningjob.job_id = bl_job.job_id )
                AND bl_runningjob.%s = :value
                """

    def format(self, res):
        """
        Format the results into the right output
        """

        form = self.formatDict(res)
        final = []
        for entry in form:
            result = {}
            result['jobId']            = entry['jobid']
            result['taskId']           = entry['taskid']
            result['name']             = entry['name']
            result['executable']       = entry['executable']
            result['events']           = entry['events']
            result['arguments']        = entry['arguments']
            result['standardInput']    = entry['standardinput']
            result['standardOutput']   = entry['standardoutput']
            result['standardError']    = entry['standarderror']
            result['inputFiles']       = entry['inputfiles']
            result['outputFiles']      = entry['outputfiles']
            result['dlsDestination']   = entry['dlsdestination']
            result['submissionNumber'] = entry['submissionnumber']
            result['closed']           = entry['closed']

            final.append(result)

        return final

    def execute(self, column, value, conn = None, transaction = False):
        """
        Load a job based on the attributes of the most recent runningJob
        """
        if type(value) == list:
            binds = value
        else:
            binds = {'value': value}

        sql = self.sql %(column)
        result = self.dbi.processData(sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
