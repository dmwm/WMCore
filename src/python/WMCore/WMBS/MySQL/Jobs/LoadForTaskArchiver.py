#!/usr/bin/env python
"""
_LoadForTaskArchiver_

MySQL implementation of Jobs.LoadForTaskArchiver

Created on Sep 5, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

from WMCore.WMBS.Job        import Job
from WMCore.WMBS.File       import File
from WMCore.DataStructs.Run import Run

from future.utils import listvalues

class LoadForTaskArchiver(DBFormatter):
    """
    _LoadForTaskArchiver_

    Retrieve file and mask data for a job given it's ID.
    """

    fileSQL = """SELECT wfd.id, wfd.lfn, wja.job AS jobid
                 FROM wmbs_file_details wfd
                 INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                 WHERE wja.job = :jobid"""

    runLumiSQL = """SELECT fileid, run, lumi FROM wmbs_file_runlumi_map
                     WHERE fileid = :fileid"""

    def execute(self, jobID, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID(s) and then format and return
        the result.
        """

        if not isinstance(jobID, list):
            jobID = [jobID]

        binds = [{"jobid": x} for x in jobID]

        if not binds:
            return []

        #First load full file information with run/lumis
        filesResult = self.dbi.processData(self.fileSQL, binds, conn = conn,
                                           transaction = transaction)
        fileList = self.formatDict(filesResult)

        #Clear duplicates
        bindDict = {}
        for result in fileList:
            bindDict[result['id']] = 1
            result['newRuns'] = []
        fileBinds = [{'fileid' : x} for x in bindDict]

        #Load file information
        if len(fileBinds):
            lumiResult = self.dbi.processData(self.runLumiSQL, fileBinds, conn = conn,
                                              transaction = transaction)
            lumiList = self.formatDict(lumiResult)
            lumiDict = {}
            for l in lumiList:
                if not l['fileid'] in lumiDict:
                    lumiDict[l['fileid']] = []
                lumiDict[l['fileid']].append(l)

            for f in fileList:
                fileRuns = {}
                if f['id'] in lumiDict:
                    for l in lumiDict[f['id']]:
                        run = l['run']
                        lumi = l['lumi']
                        try:
                            fileRuns[run].append(lumi)
                        except KeyError:
                            fileRuns[run] = []
                            fileRuns[run].append(lumi)

                for r in fileRuns:
                    newRun = Run(runNumber = r)
                    newRun.lumis = fileRuns[r]
                    f['newRuns'].append(newRun)

        filesForJobs = {}
        for f in fileList:
            jobid = f['jobid']
            if jobid not in filesForJobs:
                filesForJobs[jobid] = {}
            if f['id'] not in filesForJobs[jobid]:
                wmbsFile = File(id = f['id'])
                wmbsFile.update(f)
                for r in wmbsFile['newRuns']:
                    wmbsFile.addRun(r)
                filesForJobs[jobid][f['id']] = wmbsFile


        #Add the file information to job objects and load the masks
        jobList = [Job(id = x) for x in jobID]
        for j in jobList:
            if j['id'] in filesForJobs:
                j['input_files'] = listvalues(filesForJobs[j['id']])
            j['mask'].load(j['id'])

        return jobList
