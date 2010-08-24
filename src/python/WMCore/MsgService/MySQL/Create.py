from WMCore.Database.DBCreator import DBCreator

class CreateWMBS(DBCreator):
    
    def __init__(self, logger, dbinterface):
        DBFormatter.__init__(self, logger, dbinterface)
        self.create = {}
        self.constraints = {}
        self.create['wmbs_fileset'] = """CREATE TABLE wmbs_fileset (
                id int(11) NOT NULL AUTO_INCREMENT,
                name varchar(255) NOT NULL,
                open boolean NOT NULL DEFAULT FALSE,
                last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP,  
                PRIMARY KEY (id), UNIQUE (name))"""
        
        self.constraints['uniquewfname'] = """
            CREATE UNIQUE INDEX uniq_wf_name 
            on wmbs_workflow (name)"""
        
            
    
            