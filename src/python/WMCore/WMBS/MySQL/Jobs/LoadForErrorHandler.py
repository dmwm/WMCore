#!/usr/bin/env python
"""
_LoadForErrHandler_

MySQL implementation of Jobs.LoadForErrorHandler.
"""

from WMCore.Database.DBFormatter import DBFormatter

from WMCore.WMBS.File       import File
from WMCore.DataStructs.Run import Run

class LoadForErrorHandler(DBFormatter):
    """
    _LoadForErrorHandler_

    Retrieve meta data for a job given it's ID.  This includes the name,
    job group and last update time. It also works for the AccountantWorker
    to determine ACDC records for skipped files in successful jobs, this mode
    is used when a file selection is provided.
    """
    sql = """SELECT wmbs_job.id, jobgroup, wmbs_job.name AS name,
                    wmbs_job_state.name AS state, state_time, retry_count,
                    couch_record,  cache_dir, wmbs_location.site_name AS location,
                    outcome AS bool_outcome, fwjr_path AS fwjr_path, ww.name as workflow,
                    ww.task as task, ww.spec as spec, wmbs_users.owner as owner,
                    wmbs_users.grp as grp
             FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_workflow ww ON ww.id = wmbs_subscription.workflow
               INNER JOIN wmbs_users ON ww.owner = wmbs_users.id
               LEFT OUTER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_job.id = :jobid"""


    fileSQL = """SELECT wfd.id, wfd.lfn, wfd.filesize size, wfd.events, wfd.first_event,
                   wfd.merged, wja.job jobid,
                   wls.se_name pnn
                 FROM wmbs_file_details wfd
                 INNER JOIN wmbs_job_assoc wja ON wja.fileid = wfd.id
                 INNER JOIN wmbs_file_location wfl ON wfl.fileid = wfd.id
                 INNER JOIN wmbs_location wl ON wl.id = wfl.location
                 INNER JOIN wmbs_location_senames wls ON wls.location = wfl.location
                 WHERE wja.job = :jobid"""


    parentSQL = """SELECT parent.lfn AS lfn, wfp.child AS id
                     FROM wmbs_file_parent wfp
                     INNER JOIN wmbs_file_details parent ON parent.id = wfp.parent
                     WHERE wfp.child = :fileid """

    runLumiSQL = """SELECT fileid, run, lumi FROM wmbs_file_runlumi_map
                     WHERE fileid = :fileid"""


    def formatJobs(self, result):
        """
        _formatJobs_

        Cast the id, jobgroup and last_update columns to integers because
        formatDict() turns everything into strings.
        """

        formattedResult = DBFormatter.formatDict(self, result)

        for entry in formattedResult:
            if entry["bool_outcome"] == 0:
                entry["outcome"] = "failure"
            else:
                entry["outcome"] = "success"

            del entry["bool_outcome"]

            entry['group'] = entry['grp']
            entry['input_files'] = []

        return formattedResult

    def execute(self, jobID, fileSelection = None,
                conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        if type(jobID) == list:
            if len(jobID) < 1:
                # Nothing to do
                return []
            binds = jobID
        else:
            binds = {"jobid": jobID}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        jobList = self.formatJobs(result)

        filesResult = self.dbi.processData(self.fileSQL, binds, conn = conn,
                                           transaction = transaction)
        fileList  = self.formatDict(filesResult)
        fileBinds = []
        if fileSelection:
            fileList = filter(lambda x : x['lfn'] in fileSelection[x['jobid']], fileList)
        for x in fileList:
            # Add new runs
            x['newRuns'] = []
            # Assemble unique list of binds
            if not {'fileid': x['id']} in fileBinds:
                fileBinds.append({'fileid': x['id']})

        parentList = []
        if len(fileBinds) > 0:
            parentResult = self.dbi.processData(self.parentSQL, fileBinds, conn = conn,
                                                transaction = transaction)
            parentList   = self.formatDict(parentResult)

            lumiResult = self.dbi.processData(self.runLumiSQL, fileBinds, conn = conn,
                                              transaction = transaction)
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
                        run  = l['run']
                        lumi = l['lumi']
                        try:
                            fileRuns[run].append(lumi)
                        except KeyError:
                            fileRuns[run] = []
                            fileRuns[run].append(lumi)

                for r in fileRuns.keys():
                    newRun = Run(runNumber = r)
                    newRun.lumis = fileRuns[r]
                    f['newRuns'].append(newRun)

        filesForJobs = {}
        for f in fileList:
            jobid = f['jobid']
            if not jobid in filesForJobs.keys():
                filesForJobs[jobid] = {}
            if f['id'] not in filesForJobs[jobid].keys():
                wmbsFile = File(id = f['id'])
                wmbsFile.update(f)
                wmbsFile['locations'].add(f['pnn'])
                for r in wmbsFile['newRuns']:
                    wmbsFile.addRun(r)
                for entry in parentList:
                    if entry['id'] == f['id']:
                        wmbsFile['parents'].add(entry['lfn'])
                filesForJobs[jobid][f['id']] = wmbsFile
            else:
                # If the file is there, just add the location
                filesForJobs[jobid][f['id']]['locations'].add(f['pnn'])

        for j in jobList:
            if j['id'] in filesForJobs.keys():
                j['input_files'] = filesForJobs[j['id']].values()

        return jobList
