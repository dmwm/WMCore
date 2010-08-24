from WMCore.WMBS.MySQL.Base import MySQLBase

class CreateWMBS(MySQLBase):
    
    def __init__(self, logger, dbinterface):
        MySQLBase.__init__(self, logger, dbinterface)
        self.create = {}
        self.constraints = {}
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id int(11) NOT NULL AUTO_INCREMENT,
                name varchar(255) NOT NULL,
                open boolean NOT NULL DEFAULT FALSE,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,  
                PRIMARY KEY (id), UNIQUE (name))"""
        self.create['wmbs_fileset_files'] = """CREATE TABLE wmbs_fileset_files (
                file    int(11)      NOT NULL,
                fileset int(11) NOT NULL,
                status ENUM ("active", "inactive", "invalid"),
                insert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY(fileset) references wmbs_fileset(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(file) REFERENCES wmbs_file_details(id)
                    ON DELETE CASCADE    
                    )"""
        self.create['wmbs_file_parent'] = """CREATE TABLE wmbs_file_parent (
                child INT(11) NOT NULL,
                parent INT(11) NOT NULL,
                FOREIGN KEY (child) references wmbs_file(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (parent) references wmbs_file(id),
                UNIQUE(child, parent))"""  
        self.create['wmbs_file_details'] = """CREATE TABLE wmbs_file_details (
                id int(11) NOT NULL AUTO_INCREMENT,
                lfn     VARCHAR(255) NOT NULL,
                size    int(11),
                events  int(11),
                run     int(11),
                lumi    int(11),
                UNIQUE(lfn),
                PRIMARY KEY(id),
                INDEX (lfn))"""
        self.create['wmbs_location'] = """CREATE TABLE wmbs_location (
                id int(11) NOT NULL AUTO_INCREMENT,
                se_name VARCHAR(255) NOT NULL,
                UNIQUE(se_name),
                PRIMARY KEY(id))"""
        self.create['wmbs_file_location'] = """CREATE TABLE wmbs_file_location (
                file     int(11),
                location int(11),
                UNIQUE(file, location),
                FOREIGN KEY(file)     REFERENCES wmbs_file(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(location) REFERENCES wmbs_location(id)
                    ON DELETE CASCADE)"""
        self.create['wmbs_workflow'] = """CREATE TABLE wmbs_workflow (
                id           INT(11) NOT NULL AUTO_INCREMENT,
                spec         VARCHAR(255) NOT NULL,
                name         VARCHAR(255) NOT NULL,
                owner        VARCHAR(255) NOT NULL,
                UNIQUE(spec, name, owner),
                PRIMARY KEY (id))""" 
        self.create['wmbs_subscription'] = """CREATE TABLE wmbs_subscription (
                id      INT(11) NOT NULL AUTO_INCREMENT,
                fileset INT(11) NOT NULL,
                workflow INT(11) NOT NULL,
                type    ENUM("merge", "processing"),
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE(fileset, workflow, type),   
                PRIMARY KEY(id),
                FOREIGN KEY(fileset) REFERENCES wmbs_fileset(id)
                    ON DELETE CASCADE,
                FOREIGN KEY(workflow) REFERENCES wmbs_workflow(id)
                    ON DELETE CASCADE)"""
        self.create['wmbs_sub_files_acquired'] = """CREATE TABLE 
    wmbs_sub_files_acquired (
        subscription INT(11) NOT NULL,
        file         INT(11) NOT NULL,
        FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
            ON DELETE CASCADE,
        FOREIGN KEY (file) REFERENCES wmbs_file(id))
"""
        self.create['wmbs_sub_files_failed'] = """CREATE TABLE 
wmbs_sub_files_failed (
    subscription INT(11) NOT NULL,
    file         INT(11) NOT NULL,
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
        ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id))"""
        self.create['wmbs_sub_files_complete'] = """CREATE TABLE 
wmbs_sub_files_complete (
    subscription INT(11) NOT NULL,
    file         INT(11) NOT NULL,
    FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
        ON DELETE CASCADE,
    FOREIGN KEY (file) REFERENCES wmbs_file(id))"""
        self.create['wmbs_job'] = """CREATE TABLE wmbs_job (
                id           INT(11) NOT NULL AUTO_INCREMENT,
                subscription INT(11) NOT NULL,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                FOREIGN KEY (subscription) REFERENCES wmbs_subscription(id)
                    ON DELETE CASCADE)"""        
        self.create['wmbs_job_assoc'] = """CREATE TABLE wmbs_job_assoc (
                job    INT(11) NOT NULL,
                file   INT(11) NOT NULL,
                FOREIGN KEY (job) REFERENCES wmbs_job(id)
                    ON DELETE CASCADE,
                FOREIGN KEY (file) REFERENCES wmbs_file(id)
                    ON DELETE CASCADE)"""
        
        self.constraints['uniquewfname'] = "CREATE UNIQUE INDEX uniq_wf_name on wmbs_workflow (name)"
        self.constraints['uniquewfspecowner'] = "CREATE UNIQUE INDEX uniq_wf_spec_owner on wmbs_workflow (spec, owner)"
        
    def execute(self, fileset = None, conn = None, transaction = False):
        try:
            keys = self.create.keys()
            self.logger.debug( keys )
            
            self.dbi.processData(self.create.values(), conn = conn, transaction = transaction)
            
            keys = self.constraints.keys()
            self.logger.debug( keys )
            
            self.dbi.processData(self.constraints.values(), conn = conn, transaction = transaction)
            
            return True
        except Exception, e:
            raise e