#!/usr/bin/env python
"""
_LoadForErrHandler_

MySQL implementation of Jobs.LoadForErrorHandler.
"""

from WMCore.DataStructs.Run import Run
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.File import File

from future.utils import listvalues

class LoadForErrorHandler(DBFormatter):
    """
    _LoadForErrorHandler_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time. It also works for the AccountantWorker
    to determine ACDC records for skipped files in successful jobs, this mode
    is used when a file selection is provided.
    """
    sql = """SELECT wmbs_job.id, wmbs_job.jobgroup,
               ww.name as workflow, ww.task as task
             FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_workflow ww ON ww.id = wmbs_subscription.workflow
             WHERE wmbs_job.id = :jobid"""

    fileSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize AS size, wfd.events, wfd.first_event,
                   wfd.merged, wja.job AS jobid, wpnn.pnn
                 FROM wmbs_file_details wfd
                 INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                 LEFT OUTER JOIN wmbs_file_location wfl ON wfd.id = wfl.fileid
                 LEFT OUTER JOIN wmbs_pnns wpnn ON wpnn.id = wfl.pnn
                 WHERE wja.job = :jobid"""

    parentSQL = """SELECT parent.lfn AS lfn, wfp.child AS id
                     FROM wmbs_file_parent wfp
                     INNER JOIN wmbs_file_details parent ON parent.id = wfp.parent
                     WHERE wfp.child = :fileid AND parent.merged = 1"""

    runLumiSQL = """SELECT fileid, run, lumi, num_events FROM wmbs_file_runlumi_map
                     WHERE fileid = :fileid"""


    def getRunLumis(self, fileBinds, fileList,
                    conn=None, transaction=False):
        """
        _getRunLumis_

        Fetch run/lumi/events information for each file and append Run objects
        to the files information.
        """
        if not fileBinds:
            return

        lumiResult = self.dbi.processData(self.runLumiSQL, fileBinds, conn=conn,
                                          transaction=transaction)
        lumiList = self.formatDict(lumiResult)

        lumiDict = {}
        for l in lumiList:
            lumiDict.setdefault(l['fileid'], [])
            lumiDict[l['fileid']].append(l)

        for f in fileList:
            # Add new runs
            f.setdefault('newRuns', [])

            fileRuns = {}
            if f['id'] in lumiDict:
                for l in lumiDict[f['id']]:
                    run = l['run']
                    lumi = l['lumi']
                    numEvents = l['num_events']
                    fileRuns.setdefault(run, [])
                    fileRuns[run].append((lumi, numEvents))

            for r in fileRuns:
                newRun = Run(runNumber=r)
                newRun.lumis = fileRuns[r]
                f['newRuns'].append(newRun)
        return

    def execute(self, jobID, fileSelection=None, conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        fileSelection is a dictionary key'ed by the job id and with a list
        of lfns
        """

        if isinstance(jobID, list) and not len(jobID):
            return []
        elif isinstance(jobID, list):
            binds = jobID
        else:
            binds = [{"jobid": jobID}]

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        jobList = self.formatDict(result)

        filesResult = self.dbi.processData(self.fileSQL, binds, conn=conn,
                                           transaction=transaction)
        fileList = self.formatDict(filesResult)

        # special case for skipped files
        if fileSelection:
            fileList = [x for x in fileList if x['lfn'] in fileSelection[x['jobid']]]

        noDuplicateFiles = {}
        fileBinds = []
        for x in fileList:
            # Assemble unique list of binds
            if {'fileid': x['id']} not in fileBinds:
                fileBinds.append({'fileid': x['id']})
                noDuplicateFiles[x['id']] = x

        # only upload to not duplicate files to prevent excessive memory
        self.getRunLumis(fileBinds, listvalues(noDuplicateFiles), conn, transaction)

        parentList = []
        if fileBinds:
            parentResult = self.dbi.processData(self.parentSQL, fileBinds, conn=conn,
                                                transaction=transaction)
            parentList = self.formatDict(parentResult)

        filesForJobs = {}
        for f in fileList:
            jobid = f['jobid']
            filesForJobs.setdefault(jobid, {})

            if f['id'] not in filesForJobs[jobid]:
                wmbsFile = File(id=f['id'])
                # need to update with noDuplicateFiles since this one has run lumi information.

                wmbsFile.update(noDuplicateFiles[f["id"]])
                if f['pnn']:  # file might not have a valid location, or be Null
                    wmbsFile['locations'].add(f['pnn'])
                for r in wmbsFile.pop('newRuns'):
                    wmbsFile.addRun(r)
                for entry in parentList:
                    if entry['id'] == f['id']:
                        wmbsFile['parents'].add(entry['lfn'])
                wmbsFile.pop('pnn', None)  # not needed for anything, just remove it
                wmbsFile.pop('newlocations', None)  # not needed for anything, just remove it
                filesForJobs[jobid][f['id']] = wmbsFile
            elif f['pnn']:
                # If the file is there and it has a location, just add it
                filesForJobs[jobid][f['id']]['locations'].add(f['pnn'])

        for j in jobList:
            j.setdefault('input_files', [])
            if j['id'] in filesForJobs:
                j['input_files'] = listvalues(filesForJobs[j['id']])

        return jobList
