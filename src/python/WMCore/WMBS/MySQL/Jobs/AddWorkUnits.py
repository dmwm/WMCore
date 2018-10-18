#!/usr/bin/env python
"""
_AddWorkUnits_

MySQL implementation of Jobs.AddWorkUnits
"""
from __future__ import absolute_import, division, print_function

import logging

from WMCore.Database.DBFormatter import DBFormatter


class AddWorkUnits(DBFormatter):
    """
    Add WorkUnit associations to jobs
    """

    sql = ('INSERT INTO wmbs_job_workunit_assoc (workunit, job) '
           ' SELECT wu.id, :jobid'
           ' FROM wmbs_workunit wu'
           ' INNER JOIN wmbs_frl_workunit_assoc frla ON wu.id = frla.workunit'
           ' WHERE frla.fileid=:fileid AND frla.run=:run AND frla.lumi=:lumi')

    def getBinds(self, jobFileRunLumis):
        """
        Translate a bulk list into a number of inserts

        Args:
            jobFileRunLumis: a list of tuples of the form [(jobid, fileid, run, lumi), ...]

        Returns: N/A
        """

        binds = []

        for jid, fid, run, lumi in jobFileRunLumis:
            binds.append({'jobid': jid, 'fileid': fid, 'run': run, 'lumi': lumi})

        return binds

    def execute(self, jobid=None, fileid=None, run=None, lumi=None, jobFileRunLumis=None, conn=None, transaction=False):
        """
        Args:
            jobid: The id of a single job
            fileid: The id of a single file
            run: Run # for a single work unit
            lumi: Lumi # for a single work unit
            jobFileRunLumis: a list of tuples of the form [(jobid, fileid, run, lumi), ...]

        Returns: N/A
        """

        if jobFileRunLumis:
            binds = self.getBinds(jobFileRunLumis)
        elif jobid and fileid and run and lumi:
            binds = DBFormatter.getBinds(self, jobid=jobid, fileid=fileid, run=run, lumi=lumi)
        elif not jobFileRunLumis and isinstance(jobFileRunLumis, list):  # Empty list is ok
            return
        else:
            logging.error('Jobs.AddWorkUnits called with insufficient arguments')
            return

        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return
