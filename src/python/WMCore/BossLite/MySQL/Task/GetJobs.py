#!/usr/bin/env python
"""
_Save_

MySQL implementation of BossLite.Jobs.Save
"""

__all__ = []
__revision__ = "$Id: GetJobs.py,v 1.2 2010/04/15 20:52:22 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetJobs(DBFormatter):
    sql = """SELECT id as id, job_id as jobId, task_id as taskId,
                name as name, executable as executable, events as events,
                arguments as arguments, stdin as StandardInput,
                stdout as StandardOutput, stderr as StandardError,
                input_files as inputFiles, output_files as outputFiles,
                dls_destination as dlsDestination, submission_number as submissionNumber,
                closed as closed
                FROM bl_job
                WHERE task_id = :id
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
            result['inputFiles']       = entry['inputfiles']
            result['outputFiles']      = entry['outputfiles']
            result['dlsDestination']   = entry['dlsdestination']
            result['submissionNumber'] = entry['submissionnumber']
            result['closed']           = entry['closed']

            final.append(result)

        return final

    def execute(self, id, conn = None, transaction = False):
        """
        Load everything using the database ID
        """
        if type(id) == list:
            binds = id
        else:
            binds = {'id': id}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.format(result)
