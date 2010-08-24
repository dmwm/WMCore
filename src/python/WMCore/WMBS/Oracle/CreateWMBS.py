"""
_CreateWMBS_

Implementation of CreateWMBS for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary 
at some high value.
"""

__revision__ = "$Id: CreateWMBS.py,v 1.2 2008/10/10 17:28:25 metson Exp $"
__version__ = "$Reivison: $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class CreateWMBS(CreateWMBSBase):
    def __init__(self, logger, dbInterface):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbInterface)
        self.requiredTables.append('30wmbs_subs_type')
        
        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
             id          number(10) not null,
             name        VARCHAR(255) NOT NULL,
             open        BOOLEAN      NOT NULL DEFAULT FALSE,
             last_update TIMESTAMP    NOT NULL,
             UNIQUE (name))"""
        sequence_tables.append('wmbs_fileset') 
        
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id          number(10) not null,
             lfn          VARCHAR(255) NOT NULL,
             size         INT(11),
             events       INT(11),
             first_event  INT(11),
             last_event   INT(11))"""
        sequence_tables.append('wmbs_file_details') 
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id          number(10) not null,
             se_name VARCHAR(255) NOT NULL,
             UNIQUE(se_name))"""
        sequence_tables.append('wmbs_location') 
         
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
             id          number(10) not null,
             spec         VARCHAR(255) NOT NULL,
             name         VARCHAR(255) NOT NULL,
             owner        VARCHAR(255))"""
        sequence_tables.append('wmbs_workflow') 
        
        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
             id          number(10) not null,
             fileset     INT(11)      NOT NULL,
             workflow    INT(11)      NOT NULL,
             split_algo  VARCHAR(255) NOT NULL DEFAULT 'File',
             type        INT(11)      NOT NULL,
             last_update TIMESTAMP    NOT NULL,
             FOREIGN KEY(fileset)  REFERENCES wmbs_fileset(id)
               ON DELETE CASCADE
             FOREIGN KEY(type)     REFERENCES wmbs_subs_type(id)
               ON DELETE CASCADE
             FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
               ON DELETE CASCADE)""" 
        sequence_tables.append('wmbs_subscription') 
               
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id          number(10) not null,
             subscription INT(11)    NOT NULL,
             uid          VARCHAR(255),
             output       INT(11),
             last_update  TIMESTAMP NOT NULL,
             UNIQUE(uid),
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
             FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE)"""
        sequence_tables.append('wmbs_jobgroup') 
        
        self.create["14wmbs_job"] = \
          """CREATE TABLE wmbs_job (
             id          number(10) not null,
             jobgroup    INT(11)   NOT NULL,
             name        VARCHAR(255),
             last_update TIMESTAMP NOT NULL,
             UNIQUE(name),
             FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
               ON DELETE CASCADE)"""               
        sequence_tables.append('wmbs_job')           
          
        self.create["30wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
             id          number(10) not null,
             name VARCHAR(255) NOT NULL)"""
             
        sequence_tables.append('wmbs_subs_type')
        
        for subType in ("Processing", "Merge", "Job"):
            subTypeQuery = "INSERT INTO wmbs_subs_type (name) values ('%s')" % \
                           subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery

        for i in self.create.keys():
            self.create[i] = self.create[i].replace('INTEGER', 'number(10)')
            self.create[i] = self.create[i].replace('INT(11)', 'number(10)')
        j=50
        for i in sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
      "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100;" % seqname