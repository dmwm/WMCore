"""
_Create_

Implementation of Create for Oracle.

Inherit from CreateWMBSBase, and add Oracle specific creates to the dictionary
at some high value.

"""




from WMCore.WMBS.CreateWMBSBase import CreateWMBSBase
from WMCore.JobStateMachine.ChangeState import Transitions

class Create(CreateWMBSBase):
    """
    Class to set up the WMBS schema in an Oracle database
    """
    sequence_tables = []

    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """

        CreateWMBSBase.__init__(self, logger, dbi)

        tablespaceTable = ""
        tablespaceIndex = ""

        if params:
            if "tablespace_table" in params:
                tablespaceTable = "TABLESPACE %s" % params["tablespace_table"]
            if "tablespace_index" in params:
                tablespaceIndex = "USING INDEX TABLESPACE %s" % params["tablespace_index"]

        self.create = {}
        self.constraints = {}
        self.indexes = {}

        self.create["01"] = """CREATE TABLE Person
                (
                ID                    integer,
                Name                  varchar(100),
                DistinguishedName     varchar(500)      unique not null,
                ContactInfo           varchar(250),
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID)
                ); %s""" % tablespaceTable

        self.create["02"] = """ CREATE TABLE SchemaVersion
                (
                ID                    integer,
                SchemaVersion         varchar(100)      unique not null,
                InstanceName          varchar(100)      unique not null,
                InstanceType      varchar(10)       unique not null,
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID)
                ); %s""" % tablespaceTable

        self.create["03"] = """ CREATE TABLE DatasetPath
                (
                ID                    integer,
                Path                  varchar(500)      unique not null,
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID)
                ); %s""" % tablespaceTable

        self.create["04"] = """ CREATE TABLE DatasetParent
                (
                ID                    integer,
                ThisPath              integer   not null,
                ItsParent             integer   not null,
                CreatedBy             integer,
                CreationDate          integer,
                LastModifiedBy        integer,
                LastModificationDate  integer,
                primary key(ID),
                unique(ThisPath,ItsParent)
                ); %s""" % tablespaceTable

        self.create["05"] = """ CREATE TABLE Runs
                (
                ID                    integer,
                RunNumber             integer   unique not null,
                CreatedBy             integer,
                CreationDate          integer,
                LastModifiedBy        integer,
                LastModificationDate  integer,
                primary key(ID)
                ); %s""" % tablespaceTable

        self.create["06"] = """ CREATE TABLE LumiSection
                (
                ID                    integer,
                LumiSectionNumber     integer   not null,
                RunNumber             integer   not null,
                CreatedBy             integer,
                CreationDate          integer,
                LastModifiedBy        integer,
                LastModificationDate  integer,
                primary key(ID),
                unique(LumiSectionNumber,RunNumber)
                ); %s""" % tablespaceTable

        self.create["07"] = """ CREATE TABLE SubSystem
                (
                ID                    integer,
                Name                  varchar(500)      not null,
                Parent                varchar(500)      default 'CMS' not null,
                CreatedBy             integer,
                CreationDate          integer,
                LastModifiedBy        integer,
                LastModificationDate  integer,
                unique(Name, Parent),
                primary key(ID)
                ); %s""" % tablespaceTable

        self.create["08"] = """ CREATE TABLE RunLumiQuality
                (
                ID                    integer,
                Dataset               integer     not null,
                Run                   integer     not null,
                Lumi                  integer,
                SubSystem             integer     not null,
                DQValue               varchar(50) not null,
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID),
                unique(Dataset,Run,Lumi,SubSystem)
                ); %s""" % tablespaceTable

        self.create["09"] = """ CREATE TABLE QualityHistory
                (
                ID                    integer,
                HistoryOf             integer,
                HistoryTimeStamp      integer     not null,
                Dataset               integer     not null,
                Run                   integer     not null,
                Lumi                  integer,
                SubSystem             integer     not null,
                DQValue               varchar(50) not null,
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID),
                unique(HistoryTimeStamp,Dataset, Run,Lumi,SubSystem,DQValue)
                ); %s""" % tablespaceTable

        self.create["11"] = """ CREATE TABLE QualityVersion
                (
                ID                    integer,
                Version               varchar(500)      unique not null,
                VersionTimeStamp      integer            unique not null,
                Description           varchar(1000),
                CreationDate          integer,
                CreatedBy             integer,
                LastModificationDate  integer,
                LastModifiedBy        integer,
                primary key(ID)
                ); %s""" % tablespaceTable

        self.constraints["dq_pr_01"]="""ALTER TABLE Person ADD CONSTRAINT
                Person_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_pr_02"]="""ALTER TABLE Person ADD CONSTRAINT
                Person_LastModifiedBy_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_sv_01"]="""ALTER TABLE SchemaVersion ADD CONSTRAINT
                SchemaVersion_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_sv_02"]="""ALTER TABLE SchemaVersion ADD CONSTRAINT
                SchemaVersionLastModifiedBy_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_r_1"]="""ALTER TABLE Runs ADD CONSTRAINT
                Runs_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_2_2"]="""ALTER TABLE Runs ADD CONSTRAINT
                Runs_LastModifiedBy_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_ls_01"]="""ALTER TABLE LumiSection ADD CONSTRAINT
                LumiSection_RunNumber_FK foreign key(RunNumber) references Runs(ID)"""

        self.constraints["dq_ls_02"]="""ALTER TABLE LumiSection ADD CONSTRAINT
                LumiSection_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_ls_03"]="""ALTER TABLE LumiSection ADD CONSTRAINT
                LumiSection_LastModifiedBy_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_ss_01"]="""ALTER TABLE SubSystem ADD CONSTRAINT
                SubSystem_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_ss_02"]="""ALTER TABLE SubSystem ADD CONSTRAINT
                SubSystem_LastModifiedBy_FK foreign key(LastModifiedBy) references Person(ID)"""



        self.constraints["dq_dp_01"]="""ALTER TABLE DatasetParent ADD CONSTRAINT
                DatasetParent_ThisPath_FK foreign key(ThisPath) references DatasetPath(ID) on delete CASCADE"""

        self.constraints["dq_dp_02"]="""ALTER TABLE DatasetParent ADD CONSTRAINT
                DatasetParent_ItsParent_FK foreign key(ItsParent) references DatasetPath(ID) on delete CASCADE"""

        self.constraints["dq_dp_03"]="""ALTER TABLE DatasetParent ADD CONSTRAINT
                DatasetParent_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_dp_04"]="""ALTER TABLE DatasetParent ADD CONSTRAINT
                DatasetParent_LastModBy_FK foreign key(LastModifiedBy) references Person(ID)"""



        self.constraints["dq_rlq_01"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQuality_Dataset_FK foreign key(Dataset) references DatasetPath(ID)"""

        self.constraints["dq_rlq_02"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQuality_Run_FK foreign key(Run) references Runs(ID)"""

        self.constraints["dq_rlq_03"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQuality_Lumi_FK foreign key(Lumi) references LumiSection(ID)"""

        self.constraints["dq_rlq_04"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQuality_SubSystem_FK foreign key(SubSystem) references SubSystem(ID) on delete CASCADE"""

        self.constraints["dq_rlq_05"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQuality_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_rlq_06"]="""ALTER TABLE RunLumiQuality ADD CONSTRAINT
                RunLumiQualityLastModifiedB_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_qh_01"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistory_HistoryOf_FK foreign key(HistoryOf) references RunLumiQuality(ID)"""

        self.constraints["dq_qh_02"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistory_Run_FK foreign key(Run) references Runs(ID)"""

        self.constraints["dq_qh_03"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistory_Lumi_FK foreign key(Lumi) references LumiSection(ID)"""

        self.constraints["dq_qh_04"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistory_SubSystem_FK foreign key(SubSystem) references SubSystem(ID) on delete CASCADE"""

        self.constraints["dq_qh_05"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistory_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_qh_06"]="""ALTER TABLE QualityHistory ADD CONSTRAINT
                QualityHistoryLastModifiedB_FK foreign key(LastModifiedBy) references Person(ID)"""


        self.constraints["dq_qv_01"]="""ALTER TABLE QualityVersion ADD CONSTRAINT
                QualityVersion_CreatedBy_FK foreign key(CreatedBy) references Person(ID)"""

        self.constraints["dq_qv_02"]="""ALTER TABLE QualityVersion ADD CONSTRAINT
                QualityVersionLastModifiedB_FK foreign key(LastModifiedBy) references Person(ID)"""


        ####FIXME:--------- Some indexes may need to identified and added here

        self.create["dq_seq_01"] = """create sequence seq_person"""
        self.create["dq_seq_02"] = """create sequence seq_physicsgroup"""
        self.create["dq_seq_03"] = """create sequence seq_schemaversion"""
        self.create["dq_seq_04"] = """create sequence seq_runs"""
        self.create["dq_seq_05"] = """create sequence seq_lumisection"""
        self.create["dq_seq_06"] = """create sequence seq_subsystem"""
        self.create["dq_seq_07"] = """create sequence seq_runlumiquality"""
        self.create["dq_seq_08"] = """create sequence seq_qualityhistory"""
        self.create["dq_seq_09"] = """create sequence seq_qualityversion"""
        self.create["dq_seq_10"] = """create sequence seq_datasetpath"""
        self.create["dq_seq_11"] = """create sequence seq_datasetparent"""

        self.create["TR_001"] = """ CREATE OR REPLACE TRIGGER person_TRIG before insert on person
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_person.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_002"] = """ CREATE OR REPLACE TRIGGER dspath_TRIG before insert on datasetpath
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_datasetpath.nextval into :NEW.ID from dual;       end if;    end if; end;"""


        self.create["TR_003"] = """ CREATE OR REPLACE TRIGGER dsparent_TRIG before insert on datasetparent
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_datasetparent.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_004"] = """ CREATE OR REPLACE TRIGGER schemaversion_TRIG before insert on schemaversion
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_schemaversion.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_005"] = """ CREATE OR REPLACE TRIGGER runs_TRIG before insert on runs
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_runs.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_006"] = """ CREATE OR REPLACE TRIGGER lumisection_TRIG before insert on lumisection
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_lumisection.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_007"] = """ CREATE OR REPLACE TRIGGER subsystem_TRIG before insert on subsystem
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_subsystem.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_008"] = """ CREATE OR REPLACE TRIGGER runlumiquality_TRIG before insert on runlumiquality
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_runlumiquality.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_009"] = """ CREATE OR REPLACE TRIGGER qualityhistory_TRIG before insert on qualityhistory
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_qualityhistory.nextval into :NEW.ID from dual;       end if;    end if; end;"""

        self.create["TR_010"] = """ CREATE OR REPLACE TRIGGER qualityversion_TRIG before insert on qualityversion
                for each row begin     if inserting then       if :NEW.ID is null then          select seq_qualityversion.nextval into :NEW.ID from dual;       end if;    end if; end;"""


        self.create["TR_011"] = """ CREATE OR REPLACE TRIGGER TRTSperson BEFORE INSERT OR UPDATE ON person
                        FOR EACH ROW declare
                          unixtime integer
                             :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_012"] = """ CREATE OR REPLACE TRIGGER TRTSdatasetpath BEFORE INSERT OR UPDATE ON DatasetPath
                        FOR EACH ROW declare
                         unixtime integer
                            :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_013"] = """ CREATE OR REPLACE TRIGGER TRTSphysicsgroup BEFORE INSERT OR UPDATE ON physicsgroup
                        FOR EACH ROW declare
                          unixtime integer
                          :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                         :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_014"] = """ CREATE OR REPLACE TRIGGER TRTSschemaversion BEFORE INSERT OR UPDATE ON schemaversion
                        FOR EACH ROW declare
                          unixtime integer
                           :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_015"] = """ CREATE OR REPLACE TRIGGER TRTSruns BEFORE INSERT OR UPDATE ON runs
                        FOR EACH ROW declare
                          unixtime integer
                           :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_016"] = """ CREATE OR REPLACE TRIGGER TRTSlumisection BEFORE INSERT OR UPDATE ON lumisection
                        FOR EACH ROW declare
                          unixtime integer
                           :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_017"] = """ CREATE OR REPLACE TRIGGER TRTSsubsystem BEFORE INSERT OR UPDATE ON subsystem
                        FOR EACH ROW declare
                          unixtime integer
                             :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_018"] = """ CREATE OR REPLACE TRIGGER TRTSrunlumiquality BEFORE INSERT OR UPDATE ON runlumiquality
                        FOR EACH ROW declare
                         unixtime integer
                           :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                         :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """

        self.create["TR_019"] = """ CREATE OR REPLACE TRIGGER TRTSqualityversion BEFORE INSERT OR UPDATE ON qualityversion
                        FOR EACH ROW declare
                          unixtime integer
                           :=  (86400 * (sysdate - to_date('01/01/1970 00:00:00', 'DD/MM/YYYY HH24:MI:SS'))) - (to_number(substr(tz_offset(sessiontimezone),1,3))) * 3600 ;
                        BEGIN
                          :NEW.LASTMODIFICATIONDATE := unixtime;
                        END;
                        """
