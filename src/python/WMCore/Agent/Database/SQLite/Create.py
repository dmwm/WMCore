"""
_Create_

Implementation of CreateAgent for SQLite.

"""




from WMCore.Agent.Database.CreateAgentBase import CreateAgentBase

class Create(CreateAgentBase):
    """
    Class to set up the Agent schema in a MySQL database
    """
    def __init__(self, logger = None, dbi = None, params = None):
        """
        _init_

        Call the base class's constructor and create all necessary tables,
        constraints and inserts.
        """
        CreateAgentBase.__init__(self, logger, dbi, params)
             
        self.create["02wm_workers"] = \
          """CREATE TABLE wm_workers (
             component_id  INTEGER NOT NULL REFERENCES wm_components(id),
             name          VARCHAR(255) NOT NULL,
             last_updated  INTEGER      NOT NULL,
             state         VARCHAR(255),
             pid           INTEGER,
             last_error    INTEGER,
             error_message VARCHAR(1000),
             UNIQUE (component_id, name))"""
         
        # constraints added in table definition
        self.constraints.clear() 
    
    def execute(self, conn = None, transaction = None):
        for i in self.create.keys():
            self.create[i] = self.create[i].replace('AUTO_INCREMENT', 'AUTOINCREMENT')
            
        return CreateAgentBase.execute(self, conn, transaction)   