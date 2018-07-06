#!/usr/bin/env python
"""
_LoadForErrHandler_

MySQL implementation of Jobs.LoadForErrorHandler.
"""

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
                 INNER JOIN wmbs_file_location wfl ON wfl.fileid = wfd.id
                 INNER JOIN wmbs_pnns wpnn ON wpnn.id = wfl.pnn
                 WHERE wja.job = :jobid"""

    parentSQL = """SELECT parent.lfn AS lfn, wfp.child AS id
                     FROM wmbs_file_parent wfp
                     INNER JOIN wmbs_file_details parent ON parent.id = wfp.parent
                     WHERE wfp.child = :fileid """

    jobMask = """SELECT DISTINCT FirstEvent, LastEvent, FirstLumi, LastLumi, FirstRun, LastRun 
                     FROM wmbs_job_mask WHERE job = :jobid"""

    runLumiSQL = """SELECT fileid, run, lumi, num_events FROM wmbs_file_runlumi_map
                     WHERE fileid = :fileid"""

    runLumiSQLWithMask = """SELECT fileid, run, lumi, num_events FROM wmbs_file_runlumi_map
                         WHERE fileid = :fileid AND run = :run AND 
                         lumi >= :firstLumi AND lumi <= :lastLumi"""

    def getJobMask(self, jobid, conn=None, transaction=False):

        maskResult = self.dbi.processData(self.jobMask, {"jobid": jobid} , conn=conn, transaction=transaction)
        return self.formatDict(maskResult)

    def getMaskedRunLumis(self, fileID, mask, conn=None, transaction=False):
        # create run lumi bind:

        binds = []
        for m in mask:
            if m["firstlumi"] > -1 and m["lastlumi"] > -1 and m["firstrun"] > -1 and m["lastrun"] > -1:
                # FirstRun and LastRun should be the same but just in case
                binds.extend([{"fileid": fileID, "firstLumi": m["firstlumi"], "lastLumi": m["lastlumi"], "run": run}
                              for run in range(m["firstrun"], m["lastrun"] + 1)])
        if binds:
            runLumiResult = self.dbi.processData(self.runLumiSQLWithMask, binds, conn=conn, transaction=transaction)
        return self.formatDict(runLumiResult)

    def getNoMaskRunLumis(self, fileID, conn=None, transaction=False):
        """
        _getNoMaskRunLumis_

        Fetch run/lumi/events information for each file and append Run objects
        to the files information.
        """
        lumiResult = self.dbi.processData(self.runLumiSQL, {'fileid': fileID}, conn=conn,
                                          transaction=transaction)
        return self.formatDict(lumiResult)

    def getRunLumiWithBulkFilesWithoutMask(self, fileBinds, fileList, conn=None, transaction=False):
        """
        :param fileBinds: unique files with [{'fileid': 1234, ...}]
        :param fileList: list of file objects which will be updated with run lumi information.
        :param conn:
        :param transaction:
        :return:
        """
        lumiResult = self.dbi.processData(self.runLumiSQL, fileBinds, conn=conn,
                                          transaction = transaction)
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
                f['newRuns'] = self.formatRunLumi(lumiDict[f['id']])
        return


    def formatRunLumi(self, runLumiResult):

        fileRuns = {}
        for l in runLumiResult:
            run = l['run']
            lumi = l['lumi']
            numEvents = l['num_events']
            fileRuns.setdefault(run, [])
            fileRuns[run].append((lumi, numEvents))

        runLumiList = []
        for r in fileRuns.keys():
            newRun = Run(runNumber=r)
            newRun.lumis = fileRuns[r]
            runLumiList.append(newRun)
        return runLumiList

    def updateFilesWithRunLumiMask(self, jobid, fileObj, conn=None, transaction=False):

        mask = self.getJobMask(jobid, conn=None, transaction=False)

        if mask:
            #asssumes ther is only one first/last events mask information per job and file.
            if mask[0]['lastevent'] and mask[0]['firstevent']:
                fileObj['events'] = mask[0]['lastevent'] - mask[0]['firstevent']
                fileObj['first_event'] = mask[0]['firstevent']
            elif mask[0]['firstevent']:
                fileObj['events'] -= mask[0]['firstevent']
                fileObj['first_event'] = mask[0]['firstevent']

            runLumiList = self.getMaskedRunLumis(fileObj['id'], mask, conn=conn, transaction=transaction)

            fileObj['newRuns'] = self.formatRunLumi(runLumiList)
        else:
            runLumiList = self.getNoMaskRunLumis(fileObj['id'], conn=conn, transaction=transaction)
            fileObj['newRuns'] = self.formatRunLumi(runLumiList)

    def execute(self, jobID, fileSelection=None, maskAdded=True,  conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        fileSelection is a dictionary key'ed by the job id and with a list
        of lfns
        """

        if isinstance(jobID, list) and not len(jobID):
            return [], False
        elif isinstance(jobID, list):
            binds = jobID
        else:
            binds = [{"jobid": jobID}]

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)
        jobList = self.formatDict(result)
        for entry in jobList:
            entry.setdefault('input_files', [])

        filesResult = self.dbi.processData(self.fileSQL, binds, conn=conn,
                                           transaction=transaction)
        fileList = self.formatDict(filesResult)

        fileBinds = []
        if fileSelection:
            fileList = [x for x in fileList if x['lfn'] in fileSelection[x['jobid']]]
        for x in fileList:
            # Assemble unique list of binds
            if {'fileid': x['id']} not in fileBinds:
                fileBinds.append({'fileid': x['id']})

        parentList = []
        if len(fileBinds) > 0:
            parentResult = self.dbi.processData(self.parentSQL, fileBinds, conn=conn,
                                                transaction=transaction)
            parentList = self.formatDict(parentResult)

        # add adhoc checking wether job contains a lot of files (100 times)
        # in that case, get the bulk lumi information and add the mask later

        if not maskAdded or len(fileList) > (len(jobList) * 100):
            self.getRunLumiWithBulkFilesWithoutMask(fileBinds, fileList, conn, transaction)
            maskAdded = False

        filesForJobs = {}
        for f in fileList:
            jobid = f['jobid']
            filesForJobs.setdefault(jobid, {})

            if f['id'] not in filesForJobs[jobid]:

                if maskAdded:
                    #TODO: don't need to do this for merge job
                    # add Run lumi information for the file and the job. (masked value)
                    self.updateFilesWithRunLumiMask(jobid, f, conn, transaction)

                wmbsFile = File(id=f['id'])
                wmbsFile.update(f)
                if 'pnn' in f:  # file might not have a valid location
                    wmbsFile['locations'].add(f['pnn'])
                for r in wmbsFile.pop('newRuns'):
                    wmbsFile.addRun(r)
                for entry in parentList:
                    if entry['id'] == f['id']:
                        wmbsFile['parents'].add(entry['lfn'])
                wmbsFile.pop('pnn', None)  # not needed for anything
                filesForJobs[jobid][f['id']] = wmbsFile
            elif 'pnn' in f:
                # If the file is there and it has a location, just add it
                filesForJobs[jobid][f['id']]['locations'].add(f['pnn'])

        for j in jobList:
            if j['id'] in filesForJobs.keys():
                j['input_files'] = filesForJobs[j['id']].values()

        return jobList, maskAdded
