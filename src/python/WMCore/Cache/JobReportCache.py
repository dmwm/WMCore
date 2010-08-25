#!/usr/bin/env python
"""
_JobReportCache_

API for inserting job reports into a Couch DB instance

"""

from WMCore.Database.CMSCouch import CouchServer
from WMCore.Services.UUID import makeUUID
from ProdCommon.FwkJobRep.ReportParser import readJobReport



class JobReportCache:
    """
    _JobReportCache_


    API for adding job reports to a couch cache & querying them

    """
    def __init__(self, dbUrl = "127.0.0.1:5984", dbName = "job_report_cache"):
        self.url = dbUrl
        self.name = dbName
        self.couch = CouchServer(self.url)
        if self.name not in self.couch.listDatabases():
            self.createDatabase()
        self.database = self.couch.connectDatabase(self.name)



    def createDatabase(self):
        """
        _createDatabase_

        Initialise a new job report cache in couch

        """
        self.database = self.couch.createDatabase(self.name)
        hashViewDoc = self.database.createDesignDoc('jobs')
        hashViewDoc["views"] = { }
        #TODO: Add views
        self.database.queue( hashViewDoc )
        self.database.commit()
        return


    def createJobRecord(self, reportFile, commit = True):
        """
        _createJobRecord_

        process a job report file


        """
        reports = readJobReport(reportFile)

        for report in reports:
            couchJob = {'type': 'jobreport'}
            couchJob['_id'] = report.jobSpecId

            couchJob['files'] = {}
            couchJob['input_files'] = {}
            couchJob['analysis_files'] = {}
            couchJob['exit_code'] = report.exitCode
            couchJob['log_files'] = report.logFiles
            couchJob['removed_files'] = report.removedFiles
            couchJob['unremoved_files'] = report.unremovedFiles
            couchJob['site'] = report.siteDetails

            #TODO: Record job report errors

            for f in report.sortFiles():
                couchJob['files'][f['LFN']] = f.json()

            for f in report.inputFiles:
                couchJob['input_files'][f['LFN']] = f.json()

            for a in report.analysisFiles:
                couchJob['analysis_files'][a['FileName']] = a


            self.database.queue(couchJob, timestamp=True)
        if commit:
            self.database.commit()

        return

    def createRecords(self, *reports):
        """
        _cerateReportRecords_

        Insert a list of job reports

        """
        map(lambda x: self.createJobRecord(x, False), reports)
        self.database.commit()


