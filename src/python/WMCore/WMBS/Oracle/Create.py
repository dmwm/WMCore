"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary 
at some high value.

Remove Oracle reserved words (e.g. size, file) and revise SQL used (e.g. no BOOLEAN)
"""

__revision__ = "$Id: Create.py,v 1.4 2009/01/02 19:25:57 sfoulkes Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase

class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in an Oracle database
    """
    sequence_tables = []
    sequence_tables.append('wmbs_fileset')
    sequence_tables.append('wmbs_file_details')
    sequence_tables.append('wmbs_location')
    sequence_tables.append('wmbs_workflow') 
    sequence_tables.append('wmbs_subs_type')
    sequence_tables.append('wmbs_subscription') 
    sequence_tables.append('wmbs_jobgroup')
    sequence_tables.append('wmbs_job')
    
    def __init__(self, logger = None, dbi = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateWMBSBase.__init__(self, logger, dbi)
        self.requiredTables.append('09wmbs_subs_type')

        self.create["01wmbs_fileset"] = \
          """CREATE TABLE wmbs_fileset (
             id          number(10) not null,
             name        VARCHAR(255) not null,
             open        CHAR(1) CHECK (open IN ('0', '1' )) not null,
             last_update number(10)    not null,
             constraint pk_fileset primary key (id),
             constraint uk_filesetname unique (name))"""
             
        self.create["02wmbs_file_details"] = \
          """CREATE TABLE wmbs_file_details (
             id           number(10) not null,
             lfn          VARCHAR(255) not null,
             filesize     number(10),
             events       number(10),
             cksum        number(10),
             first_event  number(10),
             last_event   number(10),
             constraint pk_file primary key (id),
             constraint uk_filelfn unique (lfn))"""
             
        self.create["03wmbs_fileset_files"] = \
          """CREATE TABLE wmbs_fileset_files (
             fileid       number(10)   not null,
             fileset     number(10)   not null,
             insert_time number(10) not null,
             constraint fk_filesetfiles_fileset
                 FOREIGN KEY(fileset) references wmbs_fileset(id)
                    ON DELETE CASCADE,
             constraint fk_filesetfiles_file
                 FOREIGN KEY(fileid) references wmbs_file_details(id)
                    ON DELETE CASCADE)"""
             
        self.create["04wmbs_file_parent"] = \
          """CREATE TABLE wmbs_file_parent (
             child  number(10) not null,
             parent number(10) not null,
             constraint fk_fileparentage_child
                 FOREIGN KEY (child)  references wmbs_file_details(id)
                   ON DELETE CASCADE,
             constraint fk_fileparentage_parent        
                 FOREIGN KEY (parent) references wmbs_file_details(id),
             UNIQUE(child, parent))"""  
        
        self.create["05wmbs_file_runlumi_map"] = \
          """CREATE TABLE wmbs_file_runlumi_map (
             fileid   number(10) not null,
             run     number(10) not null,
             lumi    number(10) not null,
             constraint fk_runlumi_file
                 FOREIGN KEY (fileid) references wmbs_file_details(id)
                   ON DELETE CASCADE)"""
        
        self.create["06wmbs_location"] = \
          """CREATE TABLE wmbs_location (
             id          number(10) not null,
             se_name VARCHAR(255) not null,
             constraint pk_sename primary key (id),
             constraint uk_sename unique (se_name))"""
             
        self.create["07wmbs_file_location"] = \
          """CREATE TABLE wmbs_file_location (
             fileid    number(10) not null,
             location number(10) not null,
             constraint uk_sfile_location unique (fileid, location),
             constraint fk_location_file
                 FOREIGN KEY(fileid)     REFERENCES wmbs_file_details(id)
                   ON DELETE CASCADE,
             constraint fk_location_se
                 FOREIGN KEY(location) REFERENCES wmbs_location(id)
                   ON DELETE CASCADE)"""
         
        self.create["08wmbs_workflow"] = \
          """CREATE TABLE wmbs_workflow (
             id          number(10) not null,
             spec         VARCHAR(255) not null,
             name         VARCHAR(255) not null,
             owner        VARCHAR(255),
             
             constraint pk_workflow primary key (id),
             constraint uk_workflow_nameowner unique (name, owner))"""
        
        self.create["09wmbs_subs_type"] = \
          """CREATE TABLE wmbs_subs_type (
             id          number(10) not null,
             name VARCHAR(255) not null,
             constraint pk_subtype primary key (id),
             constraint uk_subtype_name unique (name))"""
             
        self.create["09wmbs_subscription"] = \
          """CREATE TABLE wmbs_subscription (
             id          number(10)   not null,
             fileset     number(10)      not null,
             workflow    number(10)      not null,
             split_algo  varchar(255) not null,
             subtype     number(10)      not null,
             last_update number(10)   not null,
             constraint fk_subs_fileset
                 FOREIGN KEY(fileset)  REFERENCES wmbs_fileset(id)
                   ON DELETE CASCADE,
             constraint fk_subs_type
                 FOREIGN KEY(subtype)     REFERENCES wmbs_subs_type(id)
                   ON DELETE CASCADE,
             constraint fk_subs_workflow           
                 FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
                   ON DELETE CASCADE,
             constraint pk_subscription primary key (id))""" 

        self.create["09wmbs_subscription_location"] = \
          """CREATE TABLE wmbs_subscription_location (
             subscription     INT(11)      NOT NULL,
             location         INT(11)      NOT NULL,
             valid            CHAR(1)    not null,
             constraint ck_valid CHECK (valid IN ( '0', '1' )),
             constraint fk_subs_loc_subscription
                 FOREIGN KEY(subscription)  REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subs_loc_location
                 FOREIGN KEY(location)     REFERENCES wmbs_location(id)
                   ON DELETE CASCADE)"""

        self.create["10wmbs_sub_files_acquired"] = \
          """CREATE TABLE wmbs_sub_files_acquired (
             subscription number(10) not null,
             fileid        number(10) not null,
             constraint fk_subsacquired_sub
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subsacquired_file
                 FOREIGN KEY (fileid)         REFERENCES wmbs_file_details(id))"""

        self.create["11wmbs_sub_files_failed"] = \
          """CREATE TABLE wmbs_sub_files_failed (
             subscription number(10) not null,
             fileid        number(10) not null,
             constraint fk_subsfailed_sub
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_subsfailed_file
                 FOREIGN KEY (fileid)       REFERENCES wmbs_file_details(id))"""


        self.create["12wmbs_sub_files_complete"] = \
          """CREATE TABLE wmbs_sub_files_complete (
          subscription number(10) not null,
          fileid        number(10) not null,
          constraint fk_subscomplete_sub
             FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
               ON DELETE CASCADE,
          constraint fk_subscomplete_file
             FOREIGN KEY (fileid)         REFERENCES wmbs_file_details(id))"""

               
        self.create["13wmbs_jobgroup"] = \
          """CREATE TABLE wmbs_jobgroup (
             id           number(10) not null,
             subscription number(10)    not null,
             guid          VARCHAR(255),
             output       number(10),
             last_update  number(10) not null,
             constraint fk_jobgroup_subscription
                 FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                   ON DELETE CASCADE,
             constraint fk_jobgroup_fileset  
                 FOREIGN KEY (output) REFERENCES wmbs_fileset(id)
                        ON DELETE CASCADE,                        
             constraint pk_jobgroup primary key (id),
             constraint uk_jobgroup_output unique (output),
             constraint uk_jobgroup_uid unique (guid))"""
             
        self.create["14wmbs_job"] = \
          """CREATE TABLE wmbs_job (
             id          number(10) not null,
             jobgroup    number(10)   not null,
             name        VARCHAR(255),
             last_update number(10) not null,
             constraint fk_job_jobgroup
                 FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
                   ON DELETE CASCADE,
             constraint pk_job primary key (id),
             constraint uk_job_name unique (name))"""               

        self.create["15wmbs_job_assoc"] = \
          """CREATE TABLE wmbs_job_assoc (
             job    number(10) not null,
             fileid  number(10) not null,
             constraint fk_jobassoc_job
                 FOREIGN KEY (job)  REFERENCES wmbs_job(id)
                   ON DELETE CASCADE,
             constraint fk_jobassoc_file
                 FOREIGN KEY (fileid) REFERENCES wmbs_file_details(id)
                   ON DELETE CASCADE)"""

        self.create["16wmbs_group_job_acquired"] = \
          """CREATE TABLE wmbs_group_job_acquired (
              jobgroup number(10) not null,
              job         number(10)     not null,
             constraint fk_jobgrpacquired_group
                 FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
                   ON DELETE CASCADE,
             constraint fk_jobgrpacquired_job
                 FOREIGN KEY (job)         REFERENCES wmbs_job(id))"""

        self.create["17wmbs_group_job_failed"] = \
          """CREATE TABLE wmbs_group_job_failed (
              jobgroup number(10) not null,
              job         number(10)     not null,
             constraint fk_jobgrpfailed_group
                 FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
                   ON DELETE CASCADE,
             constraint fk_jobgrpfailed_job
                 FOREIGN KEY (job)         REFERENCES wmbs_job(id))"""

        self.create["18wmbs_group_job_complete"] = \
          """CREATE TABLE wmbs_group_job_complete (
              jobgroup number(10) not null,
              job         number(10)     not null,
             constraint fk_jobgrpcomplete_group
                 FOREIGN KEY (jobgroup) REFERENCES wmbs_jobgroup(id)
                   ON DELETE CASCADE,
             constraint fk_jobgrpcomplete_job
                 FOREIGN KEY (job)         REFERENCES wmbs_job(id))"""
             
        self.create["19wmbs_job_mask"] = \
      """CREATE TABLE wmbs_job_mask (
          job           number(10)     not null,
          FirstEvent    number(10),
          LastEvent     number(10),
          FirstLumi     number(10),
          LastLumi      number(10),
          FirstRun      number(10),
          LastRun       number(10),
          inclusivemask CHAR(1) CHECK (inclusivemask IN ('Y', 'N')) not null,
          constraint fk_mask_job
              FOREIGN KEY (job) REFERENCES wmbs_job(id)
                ON DELETE CASCADE)"""
          
        for subType in ("Processing", "Merge"):
            subTypeQuery = """INSERT INTO wmbs_subs_type (id, name) 
                          values (wmbs_subs_type_SEQ.nextval, '%s')""" % subType
            self.inserts["wmbs_subs_type_%s" % subType] = subTypeQuery

        for i in self.create.keys():
            self.create[i] = self.create[i].replace('INTEGER', 'number(10)')
            self.create[i] = self.create[i].replace('INT(11)', 'number(10)')
        j = 50
        for i in self.sequence_tables:
            seqname = '%s_SEQ' % i
            self.create["%s%s" % (j, seqname)] = \
      "CREATE SEQUENCE %s start with 1 increment by 1 nomaxvalue cache 100" \
                    % seqname
