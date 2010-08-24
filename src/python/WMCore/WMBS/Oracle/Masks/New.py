#!/usr/bin/env python
"""
_New_

Oracle implementation of Masks.New
"""

__all__ = []



from WMCore.WMBS.MySQL.Masks.New import New as NewMasksMySQL

class New(NewMasksMySQL):
    sql = NewMasksMySQL.sql

    def getDictBinds(self, jobList, inclusivemask = True):
        binds = []
        for job in jobList:
            if inclusivemask:
                mask = 'Y'
            else:
                mask = 'N'
            binds.append({'jobid': job['id'], 'inclusivemask': mask,
                          'firstevent': job['mask']['FirstEvent'],
                          'lastevent':  job['mask']['LastEvent'],
                          'firstrun':   job['mask']['FirstRun'],
                          'lastrun':    job['mask']['LastRun'],
                          'firstlumi':  job['mask']['FirstLumi'],
                          'lastlumi':   job['mask']['LastLumi'],})

        return binds
    
    def execute(self, jobid = None, inclusivemask = None, conn = None,
                transaction = False, jobList = None):

        if jobList:
            binds = self.getDictBinds(jobList, inclusivemask)
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return self.format(result)
            
        elif jobid:
            if inclusivemask == None:
                binds = self.getBinds(jobid = jobid, inclusivemask='Y')
            else:
                binds = self.getBinds(jobid = jobid, inclusivemask = 'N')
            
            result = self.dbi.processData(self.plainsql, binds, conn = conn,
                                          transaction = transaction)
            return self.format(result)

        else:
            logging.error('Masks.New asked to create Mask with no Job ID')
            return
    
