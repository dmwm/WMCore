#!/usr/bin/env python
"""
_LoadForErrHandler_

MySQL implementation of Jobs.LoadForErrorHandler.
"""
from pprint import pprint, pformat

from WMCore.DataStructs.Run import Run
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WMBS.File import File


class LoadForErrorHandler(DBFormatter):
    """
    _LoadForErrorHandler_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time. It also works for the AccountantWorker
    to determine ACDC records for skipped files in successful jobs, this mode
    is used when a file selection is provided.
    """
    sql = """SELECT wmbs_job.id, wmbs_job.jobgroup, wmbs_job.name,
                    wmbs_job_state.name AS state, state_time, retry_count,
                    couch_record,  cache_dir, wmbs_location.site_name AS location,
                    outcome AS bool_outcome, fwjr_path AS fwjr_path, ww.name as workflow,
                    ww.task as task, ww.spec as spec
             FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_workflow ww ON ww.id = wmbs_subscription.workflow
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job.id = :jobid"""

    fileSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize AS size, wfd.events, wfd.first_event,
                   wfd.merged, wja.job AS jobid, wpnn.pnn
                 FROM wmbs_file_details wfd
                 INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                 INNER JOIN wmbs_file_location wfl ON wfl.fileid = wfd.id
                 INNER JOIN wmbs_pnns wpnn ON wpnn.id = wfl.pnn
                 WHERE wja.job = :jobid"""

    fileNoLocationSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize AS size, wfd.events,
                             wfd.first_event, wfd.merged, wja.job AS jobid
                           FROM wmbs_file_details wfd
                           INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                           WHERE wja.job = :jobid"""

    parentSQL = """SELECT parent.lfn AS lfn, wfp.child AS id
                     FROM wmbs_file_parent wfp
                     INNER JOIN wmbs_file_details parent ON parent.id = wfp.parent
                     WHERE wfp.child = :fileid """

    runLumiSQL = """SELECT fileid, run, lumi, num_events FROM wmbs_file_runlumi_map
                     WHERE fileid = :fileid"""

    def formatJobs(self, result):
        """
        _formatJobs_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """

        formattedResult = DBFormatter.formatDict(self, result)

        for entry in formattedResult:
            entry['input_files'] = []
            if entry.pop("bool_outcome") == 0:
                entry["outcome"] = "failure"
            else:
                entry["outcome"] = "success"

        return formattedResult

    def checkNoLocation(self, jobBinds, fileList,
                        conn=None, transaction=False):
        """
        _checkNoLocation_

        There might be files without any valid location (thus no rows in
        wmbs_file_location), in such cases, run a different query such that
        they can get uploaded to the ACDCServer
        :param jobBinds: list of dicts with jobid
        :param fileList: formatted result returned by fileSQL query
        :return: nothing, changes fileList in place with the non-location files.
        """
        missingJobs = []
        for job in jobBinds:
            found = False
            for fDict in fileList:
                if job["jobid"] == fDict['jobid']:
                    found = True
                    break
            if not found and job not in missingJobs:
                missingJobs.append(job)

        if not missingJobs:
            return

        filesResult = self.dbi.processData(self.fileNoLocationSQL, missingJobs,
                                           conn=conn, transaction=transaction)
        filesResult = self.formatDict(filesResult)
        fileList.extend(filesResult)
        return

    def execute(self, jobID, fileSelection=None,
                conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        fileSelection is a dictionary key'ed by the job id and with a list
        of lfns
        """

        if isinstance(jobID, list):
            if len(jobID) < 1:
                # Nothing to do
                return []
            binds = jobID
        else:
            binds = [{"jobid": jobID}]

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        jobList = self.formatJobs(result)
        print("jobList %s" % pformat(jobList))

        filesResult = self.dbi.processData(self.fileSQL, binds, conn=conn,
                                           transaction=transaction)
        fileList = self.formatDict(filesResult)
        self.checkNoLocation(binds, fileList)
        print("fileList %s" % pformat(fileList))

        fileBinds = []
        if fileSelection:
            fileList = [x for x in fileList if x['lfn'] in fileSelection[x['jobid']]]
        for x in fileList:
            # Add new runs
            x['newRuns'] = []
            # Assemble unique list of binds
            if {'fileid': x['id']} not in fileBinds:
                fileBinds.append({'fileid': x['id']})

        parentList = []
        if len(fileBinds) > 0:
            parentResult = self.dbi.processData(self.parentSQL, fileBinds, conn=conn,
                                                transaction=transaction)
            parentList = self.formatDict(parentResult)

            lumiResult = self.dbi.processData(self.runLumiSQL, fileBinds, conn=conn,
                                              transaction=transaction)
            lumiList = self.formatDict(lumiResult)
            lumiDict = {}
            for l in lumiList:
                if not l['fileid'] in lumiDict.keys():
                    lumiDict[l['fileid']] = []
                lumiDict[l['fileid']].append(l)

            for f in fileList:
                fileRuns = {}
                if f['id'] in lumiDict.keys():
                    for l in lumiDict[f['id']]:
                        run = l['run']
                        lumi = l['lumi']
                        numEvents = l['num_events']
                        fileRuns.setdefault(run, [])
                        fileRuns[run].append((lumi, numEvents))

                for r in fileRuns.keys():
                    newRun = Run(runNumber=r)
                    newRun.lumis = fileRuns[r]
                    f['newRuns'].append(newRun)

        filesForJobs = {}
        for f in fileList:
            jobid = f['jobid']
            if jobid not in filesForJobs.keys():
                filesForJobs[jobid] = {}
            if f['id'] not in filesForJobs[jobid].keys():
                wmbsFile = File(id=f['id'])
                wmbsFile.update(f)
                if 'pnn' in f:  # file might not have a valid location
                    wmbsFile['locations'].add(f['pnn'])
                for r in wmbsFile['newRuns']:
                    wmbsFile.addRun(r)
                for entry in parentList:
                    if entry['id'] == f['id']:
                        wmbsFile['parents'].add(entry['lfn'])
                filesForJobs[jobid][f['id']] = wmbsFile
            elif 'pnn' in f:
                # If the file is there and it has a location, just add it
                filesForJobs[jobid][f['id']]['locations'].add(f['pnn'])

        for j in jobList:
            if j['id'] in filesForJobs.keys():
                j['input_files'] = filesForJobs[j['id']].values()

        return jobList
