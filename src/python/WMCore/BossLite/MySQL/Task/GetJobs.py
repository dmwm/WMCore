#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Jobs.Save
"""

__all__ = []
__revision__ = "$Id: GetJobs.py,v 1.3 2010/05/09 20:03:03 spigafi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.Common.System import strToList

class GetJobs(DBFormatter):
    sql = """SELECT id as id, job_id as jobId, task_id as taskId,
                name as name, executable as executable, events as events,
                arguments as arguments, stdin as StandardInput,
                stdout as StandardOutput, stderr as StandardError,
                input_files as inputFiles, output_files as outputFiles,
                dls_destination as dlsDestination, submission_number as submissionNumber,
                closed as closed
                FROM bl_job
                WHERE task_id = :taskId
                """

    def format(self, res):
        """
        Format the results into the right output
        """

        form = self.formatDict(res)
        final = []
        for entry in form:
            result = {}
            result['id']               = entry['id']
            result['jobId']            = entry['jobid']
            result['taskId']           = entry['taskid']
            result['name']             = entry['name']
            result['executable']       = entry['executable']
            result['events']           = entry['events']
            result['arguments']        = entry['arguments']
            result['standardInput']    = entry['standardinput']
            result['standardOutput']   = entry['standardoutput']
            result['standardError']    = entry['standarderror']
            result['inputFiles']       = strToList(entry['inputfiles'])
            result['outputFiles']      = strToList(entry['outputfiles'])
            result['dlsDestination']   = strToList(entry['dlsdestination'])
            result['submissionNumber'] = entry['submissionnumber']
            result['closed']           = entry['closed']

            final.append(result)

        return final

    def execute(self, binds, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        
        return self.format(result)
