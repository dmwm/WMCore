#!/usr/bin/env python
"""
_Create_

MySQL implementation of BossLite.Jobs.Create
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.2 2010/05/09 14:55:55 spigafi Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.BossLite.Common.System import strToList, listToStr

    
class New(DBFormatter):
    sql = """INSERT INTO bl_job (job_id, task_id, name, executable, events,
                arguments, stdin, stdout, stderr, input_files, output_files,
                dls_destination, submission_number, closed)
             VALUES (:jobId, :taskId, :name, :executable, :events, :arguments,
                :standardInput, :standardOutput, :standardError, :inputFiles,
                :outputFiles, :dlsDestination, :submissionNumber, :closed)
                """

    def preFormat(self, entry):
        """
        This method maps database fields with object dictionary and 
        it translate python List and timestamps in well formatted string
        """
        
        result = {}  
        
        # result['id']               = entry['id']
        result['jobId']            = entry['jobId']
        result['taskId']           = entry['taskId']
        result['name']             = entry['name']
        result['executable']       = entry['executable']
        result['events']           = entry['events']
        result['arguments']        = entry['arguments']
        result['standardInput']    = entry['standardInput']
        result['standardOutput']   = entry['standardOutput']
        result['standardError']    = entry['standardError']
        result['inputFiles']       = listToStr(entry['inputFiles'])
        result['outputFiles']      = listToStr(entry['outputFiles'])
        result['dlsDestination']   = listToStr(entry['dlsDestination'])
        result['submissionNumber'] = entry['submissionNumber']
        result['closed']           = entry['closed']
            
        return result

    def execute(self, binds, conn = None, transaction = False):
        """
        This assumes that you are passing in binds in the same format
        as BossLite.DbObjects.Job, and that you already have an id.  It was
        too long a function for me to want to write in Perugia while
        parsing the binds
        """
        
        ppBinds = self.preFormat(binds)
        
        self.dbi.processData(self.sql, ppBinds, conn = conn,
                             transaction = transaction)
        return
    
