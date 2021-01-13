#!/usr/bin/env python
"""
_New_

MySQL implementation of Jobs.New
"""

__all__ = []



import time
import logging

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wmbs_job (jobgroup, name, state, state_time,
                                   couch_record, cache_dir, location, outcome,
                                   fwjr_path) VALUES
              (:jobgroup, :name,
               (SELECT id FROM wmbs_job_state WHERE name = 'new'),
               :state_time, :couch_record, :cache_dir,
               (SELECT id FROM wmbs_location WHERE site_name = :location),
               :outcome, :fwjr_path)"""

    getIDsql = """SELECT id as id, name as name FROM wmbs_job WHERE name= :name AND jobgroup= :jobgroup"""


    def getBinds(self, jobList):
        binds = []
        for job in jobList:
            tmpDict = {}
            tmpDict["jobgroup"]     = job.get("jobgroup")
            tmpDict["name"]         = job.get("name")
            tmpDict["couch_record"] = job.get("couch_record", None)
            tmpDict["location"]     = job.get("location", None)
            tmpDict["cache_dir"]    = job.get("cache_dir", None)
            tmpDict["state_time"]   = int(time.time())
            if job.get("outcome", 'failure') == 'success':
                tmpDict['outcome'] = 1
            else:
                tmpDict['outcome'] = 0
            tmpDict["fwjr_path"]    = job.get("fwjr", None)
            binds.append(tmpDict)

        return binds

    def format(self, input):
        result = {}
        jobList = self.formatDict(input)
        for job in jobList:
            result[job['name']] = job['id']

        return result

    def execute(self, jobgroup = None, name = None, couch_record = None, location = None, cache_dir = None,
                outcome = None, fwjr = None, conn = None, transaction = False, jobList = None):

        if outcome == None or not isinstance(outcome, str):
            outcome = 'failure'
        elif outcome.lower() == 'success':
            boolOutcome = 1
        else:
            boolOutcome = 0

        #Adding jobList enters bulk mode

        if jobList:
            binds = self.getBinds(jobList)

            self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)

            binds2 = []
            for d in binds:
                binds2.append({'name': d['name'], 'jobgroup': d['jobgroup']})

            #Now we need the IDs
            result = self.dbi.processData(self.getIDsql, binds2, conn = conn, transaction = transaction)
            return self.format(result)



        elif jobgroup and name:
            binds = {"jobgroup": jobgroup, "name": name,
                     "couch_record": couch_record, "state_time": int(time.time()),
                     "location": location, "cache_dir": cache_dir, "outcome": boolOutcome, "fwjr_path": fwjr}

            self.dbi.processData(self.sql, binds, conn = conn,
                                 transaction = transaction)
            return

        else:
            logging.error('Asked for new jobs in Jobs.New without jobgroup and name!')
            return
