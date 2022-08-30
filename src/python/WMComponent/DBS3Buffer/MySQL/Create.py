"""
_Create_DBS3Buffer_

Implementation of Create_DBSBuffer for MySQL.
"""

import threading

from WMCore.Database.DBCreator import DBCreator


class Create(DBCreator):
    def __init__(self, logger=None, dbi=None, params=None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        myThread = threading.currentThread()

        DBCreator.__init__(self, myThread.logger, myThread.dbi)

        #
        # Tables, functions, procedures and sequences
        #
        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_dataset (
                 id              INTEGER      AUTO_INCREMENT,
                 path            VARCHAR(500) COLLATE latin1_general_cs NOT NULL,
                 processing_ver  INTEGER      NOT NULL,
                 acquisition_era VARCHAR(255),
                 valid_status    VARCHAR(20),
                 global_tag      VARCHAR(255),
                 parent          VARCHAR(500),
                 prep_id         VARCHAR(255),
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_dat UNIQUE (path))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_dataset_subscription (
                 id                     INTEGER      AUTO_INCREMENT,
                 dataset_id             INTEGER      NOT NULL,
                 site                   VARCHAR(100) NOT NULL,
                 custodial              INTEGER      DEFAULT 0,
                 priority               VARCHAR(10)  DEFAULT 'Low',
                 subscribed             INTEGER      DEFAULT 0,
                 delete_blocks          INTEGER,
                 dataset_lifetime       INTEGER      DEFAULT 0 NOT NULL,
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_dat_sub UNIQUE (dataset_id, site, custodial, priority))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_algo (
                 id             INTEGER       AUTO_INCREMENT,
                 app_name       VARCHAR(100),
                 app_ver        VARCHAR(100),
                 app_fam        VARCHAR(100),
                 pset_hash      VARCHAR(700),
                 config_content LONGTEXT,
                 in_dbs         INTEGER,
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_alg UNIQUE (app_name, app_ver, app_fam, pset_hash))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_algo_dataset_assoc (
                 id         INTEGER AUTO_INCREMENT,
                 algo_id    INTEGER NOT NULL,
                 dataset_id INTEGER NOT NULL,
                 in_dbs     INTEGER DEFAULT 0,
                 PRIMARY KEY (id))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_workflow (
                 id                           INTEGER       AUTO_INCREMENT,
                 name                         VARCHAR(700),
                 task                         VARCHAR(700),
                 block_close_max_wait_time    INTEGER,
                 block_close_max_files        INTEGER,
                 block_close_max_events       INTEGER,
                 block_close_max_size         BIGINT,
                 completed                    INTEGER       DEFAULT 0,
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_wor UNIQUE (name, task))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_file (
                 id                    INTEGER      AUTO_INCREMENT,
                 lfn                   VARCHAR(500) NOT NULL,
                 filesize              BIGINT,
                 events                BIGINT UNSIGNED,
                 dataset_algo          INTEGER      NOT NULL,
                 block_id              INTEGER,
                 status                VARCHAR(20),
                 in_phedex             INTEGER      DEFAULT 0,
                 workflow              INTEGER,
                 LastModificationDate  INTEGER,
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_fil UNIQUE (lfn))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_file_parent (
                 child  INTEGER NOT NULL,
                 parent INTEGER NOT NULL,
                 CONSTRAINT pk_dbs_fil_par PRIMARY KEY (child, parent))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_file_runlumi_map (
                 filename    INTEGER NOT NULL,
                 run         INTEGER NOT NULL,
                 lumi        INTEGER NOT NULL,
                 num_events  BIGINT UNSIGNED)"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_location (
                 id       INTEGER      AUTO_INCREMENT,
                 pnn  VARCHAR(255) NOT NULL,
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_loc UNIQUE (pnn))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_file_location (
                 filename INTEGER NOT NULL,
                 location INTEGER NOT NULL,
                 CONSTRAINT pk_dbs_fil_loc PRIMARY KEY (filename, location))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_block (
                 id           INTEGER      AUTO_INCREMENT,
                 dataset_id   INTEGER      NOT NULL,
                 blockname    VARCHAR(250) NOT NULL,
                 location     INTEGER      NOT NULL,
                 create_time  INTEGER,
                 status       VARCHAR(20),
                 deleted      INTEGER      DEFAULT 0,
                 rule_id      VARCHAR(40)  NOT NULL DEFAULT '0',
                 PRIMARY KEY (id),
                 CONSTRAINT uq_dbs_blo UNIQUE (blockname, location))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_checksum_type (
                 id   INTEGER      AUTO_INCREMENT,
                 type VARCHAR(255),
                 PRIMARY KEY (id))"""

        self.create[len(self.create)] = \
            """CREATE TABLE dbsbuffer_file_checksums (
                 fileid  INTEGER,
                 typeid  INTEGER,
                 cksum   VARCHAR(100),
                 CONSTRAINT pk_dbs_fil_che PRIMARY KEY (fileid, typeid))"""

        #
        # Indexes
        #
        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_che_1 ON dbsbuffer_file_checksums (typeid)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_che_2 ON dbsbuffer_file_checksums (fileid)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_dat_sub_1 ON dbsbuffer_dataset_subscription (dataset_id)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_par_1 ON dbsbuffer_file_parent (child)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_par_2 ON dbsbuffer_file_parent (parent)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_blo_1 ON dbsbuffer_block (location)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_blo_2 ON dbsbuffer_block (dataset_id)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_alg_ass_1 ON dbsbuffer_algo_dataset_assoc (dataset_id)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_alg_ass_2 ON dbsbuffer_algo_dataset_assoc (algo_id)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_run_1 ON dbsbuffer_file_runlumi_map (filename)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_loc_1 ON dbsbuffer_file_location (location)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_loc_2 ON dbsbuffer_file_location (filename)"""

        self.indexes[len(self.indexes)] = \
            """CREATE INDEX idx_dbs_fil_1 ON dbsbuffer_file (workflow)"""

        #
        # Constraints
        #
        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_checksums
                 ADD CONSTRAINT fk_file_checksums_typeid
                 FOREIGN KEY (typeid)
                 REFERENCES dbsbuffer_checksum_type(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_checksums
                 ADD CONSTRAINT fk_file_checksums_fileid
                 FOREIGN KEY (fileid)
                 REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_dataset_subscription
                 ADD CONSTRAINT fk_dsetsubscription_datasetid
                 FOREIGN KEY (dataset_id)
                 REFERENCES dbsbuffer_dataset(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_parent
                 ADD CONSTRAINT fk_file_parent_child
                 FOREIGN KEY (child)
                 REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_parent
                 ADD CONSTRAINT fk_file_parent_parent
                 FOREIGN KEY (parent)
                 REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_block
                 ADD CONSTRAINT fk_block_location
                 FOREIGN KEY (location)
                 REFERENCES dbsbuffer_location(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_block
                 ADD CONSTRAINT fk_block_dataset_id
                 FOREIGN KEY (dataset_id)
                 REFERENCES dbsbuffer_dataset(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_algo_dataset_assoc
                 ADD CONSTRAINT fk_algodset_assoc_dataset_id
                 FOREIGN KEY (dataset_id)
                 REFERENCES dbsbuffer_dataset(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_algo_dataset_assoc
                 ADD CONSTRAINT fk_algodset_assoc_algo_id
                 FOREIGN KEY (algo_id)
                 REFERENCES dbsbuffer_algo(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_runlumi_map
                 ADD CONSTRAINT fk_file_runlumi_filename
                 FOREIGN KEY (filename)
                 REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_location
                 ADD CONSTRAINT fk_file_location_location
                 FOREIGN KEY (location)
                 REFERENCES dbsbuffer_location(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file_location
                 ADD CONSTRAINT fk_file_location_filename
                 FOREIGN KEY (filename)
                 REFERENCES dbsbuffer_file(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file
                 ADD CONSTRAINT fk_file_workflow
                 FOREIGN KEY (workflow)
                 REFERENCES dbsbuffer_workflow(id)
                 ON DELETE CASCADE"""

        self.constraints[len(self.constraints)] = \
            """ALTER TABLE dbsbuffer_file
                 ADD CONSTRAINT fk_file_dataset_algo
                 FOREIGN KEY (dataset_algo)
                 REFERENCES dbsbuffer_algo_dataset_assoc(id)
                 ON DELETE CASCADE"""

        checksumTypes = ['cksum', 'adler32', 'md5']
        for i in checksumTypes:
            checksumTypeQuery = """INSERT INTO dbsbuffer_checksum_type
                                   (type)
                                   VALUES ('%s')
                                   """ % (i)
            self.inserts["wmbs_checksum_type_%s" % (i)] = checksumTypeQuery

        for i in self.create:
            self.create[i] += " ENGINE=InnoDB ROW_FORMAT=DYNAMIC"
