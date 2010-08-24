"""
Oracle implementation of Jobs.New
"""

from WMCore.WMBS.MySQL.Jobs.New import New as NewJobMySQL

class New(NewJobMySQL):
    sql = sql = [""" insert into wmbs_job (id, jobgroup, name, last_update) 
                     values (wmbs_job_SEQ.nextval, :jobgroup, :name, 
                             :timestamp)""",
                 """insert into wmbs_job_mask (job, inclusivemask) values (
                    (select id from wmbs_job 
                     where jobgroup = :jobgroup and name = :name), 'Y')
                    """,
                 """ select id from wmbs_job 
                     where jobgroup = :jobgroup and name = :name """]
    
    def execute(self, jobgroup=0, name=None, conn = None, transaction = False):
        
        # need to fix bind timestamp is not *2
        binds = self.getBinds(jobgroup=jobgroup, name=name, 
                              timestamp=self.timestamp())
        result = self.dbi.processData(self.sql[0], binds)
        
        binds = self.getBinds(jobgroup=jobgroup, name=name)
        result = self.dbi.processData(self.sql[1], binds)
        
        binds = self.getBinds(jobgroup=jobgroup, name=name)
        result = self.dbi.processData(self.sql[2], binds)
        
        return self.format(result)